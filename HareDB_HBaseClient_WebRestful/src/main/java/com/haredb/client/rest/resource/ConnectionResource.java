package com.haredb.client.rest.resource;

import java.util.List;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.haredb.hbaseclient.core.Connection;
import com.haredb.hbaseclient.core.KerberosPrincipal;
import com.haredb.adapter.bean.QueueSettingObjBean;
import com.haredb.adapter.bean.QueueTableStatusBean;
import com.haredb.adapter.jobstatus.QueueService;
import com.haredb.adapter.jobstatus.UIQueueService;
import com.haredb.client.facade.bean.ConnectionBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.util.ConnectionUtil;
import com.haredb.hbase.bulkload.BulkloadObserver;
import com.haredb.hbase.bulkload.bean.BulkloadStatus;
import com.haredb.hbase.metastore.bean.HareDBTaskQueueBean;
import com.haredb.hbase.metastore.service.HareDBTaskQueueService;

@Path("connect")
public class ConnectionResource {

	private static Logger logger = Logger.getLogger(ConnectionResource.class);
	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public MessageInfo connect(@Context HttpServletRequest request, ConnectionBean connectionBean){
		
		ConnectionUtil connUtil = new ConnectionUtil(connectionBean);
		MessageInfo info = connUtil.resolveBean();
		
		
		/* add queue control check----------------*/
		try {
			checkClusterQueueStatus(connectionBean);
		} catch (Exception e) {
			info.setStatus(MessageInfo.ERROR);
			info.setException(e.getMessage());
		}
		/* queue control check end----------------*/
		
		if(info.getStatus().equals(MessageInfo.SUCCESS)) {
			request.getSession().setAttribute(ConnectionUtil.connectionSessionName, connUtil.getConn());
			request.getSession().setAttribute(ConnectionUtil.hiveMetaDataSessionName, connUtil.getMetaConn());
		}
		return info;
	}
	
	
	
//	private static final String queueFilePropKey = "queueFilePath"; 
	
	/**
	 * 
	 *check queue file's path , and if job not running . fix queue status. 
	 */
	private void checkClusterQueueStatus(ConnectionBean cBean) throws Exception {
		/* change fade cBean to conn */
		Connection conn = getHBaseClientCoreConnection(cBean);
		conn.create();
		UIQueueService uiQueueService = new UIQueueService(conn.getConfig());
		
		logger.info("== start to get all queue info ==");
		List<QueueTableStatusBean> queueList = uiQueueService.getAllFileListMapTable(UIQueueService.JOB_STATUS_FILE_PATH);
		logger.info("== Get queue info finish ==");
		
		HareDBTaskQueueService metaService = new HareDBTaskQueueService();
		
		/* check meta is exists*/
		logger.info("== Start to check meta ==");
		if(!metaService.IsHBaseMetaTableExist(conn)){
			logger.info("== meta table not exist, start to create ==");
			metaService.createHBaseMetaTable(conn);
			logger.info("== meta table create finish ==");
		}
		
		
		logger.info("== Start to check queue status ==");
		for (QueueTableStatusBean singleQueue : queueList) {
			logger.info("== Table: "+singleQueue.getTableName()+" ==");
			HareDBTaskQueueBean metaBean = metaService.LoadData(conn, singleQueue.getTableName());
			if(metaBean.getStatus().equals(HareDBTaskQueueBean.StatusProcess)){
				String jobName = "";
				QueueSettingObjBean queuefirstjob = singleQueue.getFileBeanList().get(0);
				if(queuefirstjob.getJobType().equals(QueueService.JOBTYPE_BULK)){
					jobName = queuefirstjob.getBulkInfo().getJobName();
				}else{
					jobName = queuefirstjob.getIndexSetting().getJobName();
				}
				BulkloadObserver observer = new BulkloadObserver(conn);
				BulkloadStatus status = observer.getJobStatus(jobName);
				if(status.getJobStatus()!=null && status.getJobStatus().equals(BulkloadObserver.RUNNING)){
					logger.info("== Another client running this queue. ==");
					continue;
				}else{
					logger.info("== Queue stopped, change status to Failed. ==");
					metaBean.setStatus(HareDBTaskQueueBean.StatusFailed);
					metaService.changeTaskStatus(conn, metaBean);					
				}
			}else{
				logger.info("== status OK ==");
			}	
		}
		logger.info("== check queue status end. ==");
	}
	
	/** 
	 * by fade connection bean to hareDB Connection
	 * */
	private Connection getHBaseClientCoreConnection(ConnectionBean connectionBean){
		Connection hbaseConn = null;
		hbaseConn = new Connection(connectionBean.getZookeeperHost(), connectionBean.getZookeeperPort());
		hbaseConn.setName(connectionBean.getName());
		hbaseConn.setJobTrackerHostPort(connectionBean.getJobTrackerHostPort());
		hbaseConn.setNameNodeHostPort(connectionBean.getNameNodeHostPort());
		hbaseConn.setRmAddressHostPort(connectionBean.getRmAddressHostPort());
		hbaseConn.setRmAdminAddressHostPort(connectionBean.getRmAdminAddressHostPort());
		hbaseConn.setRmResourceTrackerAddressHostPort(connectionBean.getRmResourceTrackerAddressHostPort());
		hbaseConn.setRmSchedulerAddressHostPort(connectionBean.getRmSchedulerAddressHostPort());
		hbaseConn.setMrJobhistoryAddress(connectionBean.getMrJobhistoryAddress());
		hbaseConn.setSolrZKHosts(connectionBean.getSolrZKHosts());
		hbaseConn.setRegisterCoprocessorHdfsPath(connectionBean.getRegisterCoprocessorHdfsPath());
		if(connectionBean.isEnableKerberos()){
			KerberosPrincipal kerberosPrincipal = new KerberosPrincipal(
					connectionBean.getHbaseMasterPrincipal(), connectionBean.getHbaseRegionServerPrincipal(),
					connectionBean.getDfsNameNodePrincipal(), connectionBean.getDfsDataNodePrincipal(),
					connectionBean.getHiveMetaStorePrincipal(), connectionBean.getYarnResourceMgrPrincipal(), 
					connectionBean.getYarnNodeMgrPrincipal());
			hbaseConn.setKerberosPrincipal(kerberosPrincipal);
		}
		return hbaseConn;
		
	}
	
}
