package com.haredb.client.facade.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.haredb.HareQLConfiguration;
import com.haredb.client.facade.bean.HareQLResultStatusBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.hbase.coprocessor.rpc.QueryJobConf;
import com.haredb.hbaseclient.core.Connection;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumHiveMetaStoreConnectType;
import com.haredb.hive.ql.HareDriver;
import com.haredb.hive.ql.client.MetaDataAdapter;

public class HareQueryHareQL extends HareContrivance{
	private Connection connection;
	private HiveMetaConnectionBean hiveMetaConnectionBean;
	private String fileName="result.txt";
	
	public HareQueryHareQL(Connection connection, HiveMetaConnectionBean hiveMetaConnectionBean){
		super(connection);
		this.connection = connection;
		this.hiveMetaConnectionBean = hiveMetaConnectionBean;
	}
	public HareQLResultStatusBean executeHareQL(String tempFilePath, String sql, int page, int limit){
		HiveConf hiveConf = this.getHiveConf();
		if(tempFilePath != null){
			hiveConf.set(HareQLConfiguration.QUERYTEMPFILEPATH, tempFilePath);
		}
		return this.runHareQL(hiveConf, sql, page, limit);
	}
	
	private HareQLResultStatusBean runHareQL(HiveConf hiveConf, String sql, int page, int limit){
		 HareQLResultStatusBean resultStatus = new HareQLResultStatusBean();
		 String resultFilePath = null;
			try{
				if(hiveConf.get(HareQLConfiguration.QUERYTEMPFILEPATH) != null) {
					resultFilePath = hiveConf.get(HareQLConfiguration.QUERYTEMPFILEPATH);
				}
				long startTime = System.currentTimeMillis();
				List<String> datas = new ArrayList<String>();
				
		        
		        CliSessionState ss = new CliSessionState(hiveConf);
		        SessionState.start(ss);
		      
		        HareDriver driver = new HareDriver(hiveConf);
		
		        CommandProcessorResponse res = driver.run(sql);
		        Schema s = driver.getSchema();
		        ArrayList<String> list = new ArrayList<String>();
		        long count = 0;
		        try {
		            while (driver.getResults(list)) {
		                for (String r : list) {
		                   datas.add(r);
		                   count++;
		                }
		                list.clear();
		            }
		        } catch (Exception e) {
		            e.printStackTrace();
		        }
		        ss.close();
		        long endTime = System.currentTimeMillis();
		        List<String> heads = new ArrayList<String>();
		        if (s != null) {
		            List<FieldSchema> schema = s.getFieldSchemas();
		            if ((schema != null) && (!schema.isEmpty())) {
		                for (int pos = 0; pos < schema.size(); pos++) {
		                    heads.add(schema.get(pos).getName());
		                }
		            }
		        }
		        
		        resultStatus.setResults(datas);
				resultStatus.setResponseTime(endTime - startTime);
				resultStatus.setRowSize(count);
				resultStatus.setHeads(heads);
				resultStatus.setStatus(MessageInfo.SUCCESS);
				resultStatus.setFileSize(driver.getTempFolderFileSize());
			}catch(Exception e){
				resultStatus.setStatus(MessageInfo.ERROR);
				resultStatus.setException(e.getMessage());
				return resultStatus;
			} finally {
				if(resultFilePath != null) {
					super.writeFileToHdfs(resultStatus, resultFilePath, this.fileName, false);
				}
			}
			return resultStatus;
	}
	
	private HiveConf getHiveConf(){
	    HiveConf hiveConf = new HiveConf();

        hiveConf.set(Connection.zookeeperHostString, this.connection.getZookeeperHost());
        hiveConf.set(Connection.zookeeperPortString, this.connection.getZookeeperPort());
        hiveConf.set(Connection.nameNodeHostPortString, this.connection.getNameNodeHostPort());
        hiveConf.set(Connection.RMAddressHostPortString, this.connection.getRmAddressHostPort());
        hiveConf.set(Connection.RMSchedulerAddressHostPortString, this.connection.getRmSchedulerAddressHostPort());
        hiveConf.set(Connection.RMResourceTrackerAddressHostPortString, this.connection.getRmResourceTrackerAddressHostPort());
        hiveConf.set(Connection.RMAdminAddressHostPortString, this.connection.getRmAdminAddressHostPort());
        hiveConf.set(Connection.RMMRframeworkname, "yarn");
        hiveConf.set(Connection.MRJobhistoryAddress, this.connection.getMrJobhistoryAddress());
        hiveConf.set(Connection.YarnNodemanagerAuxServices, this.connection.getYarnNodeManagerAuxServices());
        hiveConf.set(MetaDataAdapter.HIVEDBNAME, "default");
        
        if(this.hiveMetaConnectionBean.getHiveConnType().equals(EnumHiveMetaStoreConnectType.LOCAL)){
	    	hiveConf.set(MetaDataAdapter.CONNECTIONURL, hiveMetaConnectionBean.getMetaStoreConnectURL());
			hiveConf.set(MetaDataAdapter.CONNECTIONDRIVERNAME, hiveMetaConnectionBean.getMetaStoreConnectDriver());
			hiveConf.set(MetaDataAdapter.CONNECTIONUSERNAME,	hiveMetaConnectionBean.getMetaStoreConnectUserName());	
			hiveConf.set(MetaDataAdapter.CONNECTIONPASSWORD,	hiveMetaConnectionBean.getMetaStoreConnectPassword());
        }else if(this.hiveMetaConnectionBean.getHiveConnType().equals(EnumHiveMetaStoreConnectType.REMOTE)){
    		hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveMetaConnectionBean.getMetaUris());
			hiveConf.setBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI, false);
        }else{
        	hiveConf.setBoolean(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS.varname, true);
			hiveConf.set("hive.metastore.authorization.storage.checks", "false");			
			hiveConf.set("hive.metastore.warehouse.dir", hiveMetaConnectionBean.getEmbedPath() + "/hive/warehouse/");			
			hiveConf.set("hive.metastore.warehouse.dir", hiveMetaConnectionBean.getEmbedPath() + "/hive/warehouse/");
			hiveConf.set("hive.exec.local.scratchdir", hiveMetaConnectionBean.getEmbedPath() +  "/hive/tmp/hive");
			hiveConf.set("hive.querylog.location", hiveMetaConnectionBean.getEmbedPath() + "/hive/tmp/user/");
			hiveConf.set("hive.log.dir", hiveMetaConnectionBean.getEmbedPath() + "/hive/tmp/user/");
        }
       
        hiveConf.set(QueryJobConf.hbaseRpcTimeOut, "9999999");
        hiveConf.set(QueryJobConf.hbaseClientRetryNumber, "1");
        hiveConf.set(HareQLConfiguration.SECONDRESULTBEANSPLITSIZE, "500");
        hiveConf.set(HareQLConfiguration.FIRSTRESULTBEANSPLITSIZE, "1000");
        return hiveConf;
	}
}
