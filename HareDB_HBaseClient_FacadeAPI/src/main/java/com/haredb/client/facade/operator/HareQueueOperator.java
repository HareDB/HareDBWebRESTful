package com.haredb.client.facade.operator;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.haredb.adapter.jobstatus.QueueService;
import com.haredb.adapter.jobstatus.UIQueueService;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.QueueBean;
import com.haredb.client.facade.bean.QueueStatusBean;
import com.haredb.hbase.metastore.bean.HareDBTaskQueueBean;
import com.haredb.hbase.metastore.service.HareDBTaskQueueService;
import com.haredb.hbaseclient.core.Connection;

public class HareQueueOperator  extends HareContrivance{

	private QueueBean qBean;

	public HareQueueOperator(Connection connection, QueueBean qBean){
		super(connection);
		this.qBean=qBean;
	}
	
	public QueueStatusBean initMetaTable() throws Exception{
		String tableName 					= qBean.getTableName();
		
		QueueStatusBean statusBean = new QueueStatusBean();
		statusBean.setTableName(tableName);
		HareDBTaskQueueService metaService = new HareDBTaskQueueService();
		HareDBTaskQueueBean metaBean; 
		if(!metaService.IsHBaseMetaTableExist(connection)){			
			metaService.createHBaseMetaTable(connection);
		}
		metaBean = metaService.LoadData(connection, tableName);
		statusBean.setQueueStatus(metaBean.getStatus());
		statusBean.setStatus(MessageInfo.SUCCESS);
		return statusBean;
	}
	
	
	public QueueStatusBean restartQueue() throws Exception{
		QueueStatusBean statusBean = new QueueStatusBean();
		String tableName 					= qBean.getTableName();
		statusBean.setTableName(tableName);
		QueueService service = new QueueService(connection.getConfig());

		service.reStartJob(tableName);
		statusBean.setStatus(MessageInfo.SUCCESS);
		
		return statusBean;
	}
	
	public QueueStatusBean froceChangeToComplete() throws Exception{
		String tableName 					= qBean.getTableName();
		String collectionName			= tableName;
		QueueStatusBean statusBean = new QueueStatusBean();
		statusBean.setTableName(tableName);
		HareDBTaskQueueService metaService = new HareDBTaskQueueService();
		HareDBTaskQueueBean metaBean;
		
		metaBean = metaService.LoadData(new Connection(connection.getConfig()), collectionName);
		metaBean.setStatus(HareDBTaskQueueBean.StatusComplete);
		metaService.changeTaskStatus(new Connection(connection.getConfig()), metaBean);
		statusBean.setQueueStatus(HareDBTaskQueueBean.StatusComplete);
		statusBean.setStatus(MessageInfo.SUCCESS);
		
		return statusBean;
	}
	
	public QueueStatusBean getStatus() throws Exception{
		String tableName 					= qBean.getTableName();
		String collectionName			= tableName;
		
		QueueStatusBean statusBean = new QueueStatusBean();
		statusBean.setTableName(tableName);
		HareDBTaskQueueService metaService = new HareDBTaskQueueService();
		HareDBTaskQueueBean metaBean;
		metaBean = metaService.LoadData(new Connection(connection.getConfig()), collectionName);
		statusBean.setQueueStatus(metaBean.getStatus());
		statusBean.setStatus(MessageInfo.SUCCESS);
		return statusBean;
	}
	
	public QueueStatusBean dropQueue() throws IOException{
		String tableName 					= qBean.getTableName();
		QueueStatusBean statusBean = new QueueStatusBean();
		statusBean.setTableName(tableName);
		QueueService service = new QueueService(connection.getConfig());
		service.dropQueue(tableName);
		statusBean.setStatus(MessageInfo.SUCCESS);
		return statusBean;
	}
	
	
	public QueueStatusBean getQueueJobFiles() throws IOException{
		QueueStatusBean statusBean = new QueueStatusBean();
		String tableName 					= qBean.getTableName();
		statusBean.setTableName(tableName);
		FileSystem fs = FileSystem.get(connection.getConfig());
		FileStatus[] tableQueueFiles = fs.listStatus(new Path(UIQueueService.JOB_STATUS_FILE_PATH + tableName));
		ArrayList<String> queueFiles = new ArrayList<String>();
		/* get all queue file on single table */
		for (FileStatus queueFile : tableQueueFiles) {
			queueFiles.add(queueFile.getPath().toString());
		}
		statusBean.setQueueFiles(queueFiles);
		statusBean.setStatus(MessageInfo.SUCCESS);

		return statusBean;
	}
	
	
	public QueueStatusBean deleteQueueJob() throws IOException{
		String tableName 					= qBean.getTableName();
		QueueStatusBean statusBean = new QueueStatusBean();
		statusBean.setTableName(tableName);
		QueueService service = new QueueService(connection.getConfig());
		service.deleteQueueFile(qBean.getQueueFileName());
		statusBean.setStatus(MessageInfo.SUCCESS);
		return statusBean;
	}
	
	
}
