package com.haredb.client.facade.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

import com.haredb.HareQLConfiguration;
import com.haredb.client.facade.bean.HareQLResultStatusBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.hbaseclient.core.Connection;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean;
import com.haredb.hive.ql.HareDriver;

public class HareQueryHareQL extends HareContrivance{
	private HiveMetaConnectionBean hiveMetaConnectionBean;
	private String fileName="result.txt";
	private HiveConf hiveConf = null;
	
	public HareQueryHareQL(Connection connection, HiveMetaConnectionBean hiveMetaConnectionBean){
		super(connection);
		this.hiveMetaConnectionBean = hiveMetaConnectionBean;
		initHiveConf();
	}
	
	public HareQLResultStatusBean executeHareQL(String tempFilePath, String sql, int page, int limit) throws Exception{
		if(tempFilePath != null){
			hiveConf.set(HareQLConfiguration.QUERYTEMPFILEPATH, tempFilePath);
		}
		return this.runHareQL(sql, page, limit);
	}
	
	/**
	 * Execute HareQL with new thread
	 * 
	 * @param tempFilePath
	 * @param sql
	 * @param page
	 * @param limit
	 * @return
	 * @throws Exception
	 */
	public HareQLResultStatusBean submitHareQL(final String tempFilePath, final String sql, final int page, final int limit) throws Exception{
		HareQLResultStatusBean resultStatus = new HareQLResultStatusBean();
		if(tempFilePath != null){
			hiveConf.set(HareQLConfiguration.QUERYTEMPFILEPATH, tempFilePath);
		}
		long startTime = System.currentTimeMillis();
		Thread bkLoadThread = new Thread(new Runnable() {
			@Override
			public void run() {
				
				try {
					runHareQL(sql, page, limit);
				}catch(Exception e) {
					MessageInfo info = new MessageInfo();
					info.setStatus(MessageInfo.ERROR);
					info.setException(printStackTrace(e));
					try {
						writeFileToHdfs(info, tempFilePath,fileName,true);
					} catch (Exception e1) {
						throw new RuntimeException(e1);
					}
				}	
			}
			
		});
		bkLoadThread.start();
		long endTime = System.currentTimeMillis();
		
		resultStatus.setStatus(MessageInfo.SUCCESS);
		resultStatus.setResponseTime(endTime - startTime);
		return resultStatus;
	}
	
	/**
	 * get Hare Query Status
	 * 
	 * @param tempFilePath
	 * @return
	 */
	public HareQLResultStatusBean getHareQLStatus(String tempFilePath) {
		HareQLResultStatusBean resultStatus = new HareQLResultStatusBean();
		long startTime = System.currentTimeMillis();
		try {
			if (tempFilePath == null) {
				resultStatus.setStatus(MessageInfo.ERROR);
				resultStatus.setException("File Path is null !");
			} else {
				boolean result = this.checkFileExist(tempFilePath,
						this.fileName);
				if (result == false) {
					resultStatus.setStatus(MessageInfo.RUNNING);
				} else {
					resultStatus.setStatus(MessageInfo.SUCCESS);
				}
			}
			
		} catch (Exception e) {
			resultStatus.setStatus(MessageInfo.ERROR);
			resultStatus.setException(printStackTrace(e));
		}
		long endTime = System.currentTimeMillis();
		resultStatus.setResponseTime(endTime - startTime);
		return resultStatus;
	}
	
	private HareQLResultStatusBean runHareQL(String sql, int page, int limit) throws Exception{
		 HareQLResultStatusBean resultStatus = new HareQLResultStatusBean();
		 String resultFilePath = null;
			try{
				if(hiveConf.get(HareQLConfiguration.QUERYTEMPFILEPATH) != null) {
					resultFilePath = hiveConf.get(HareQLConfiguration.QUERYTEMPFILEPATH);
				}
				long startTime = System.currentTimeMillis();
				List<String> datas = new ArrayList<String>();
		        
		        CliSessionState ss = new CliSessionState(hiveConf);
		        UserGroupInformation.setConfiguration(hiveConf);
		        SessionState.start(ss);
		      
		        HareDriver driver = new HareDriver(hiveConf);
		
		        CommandProcessorResponse res = driver.run(sql);
		        Schema s = driver.getSchema();
		        ArrayList<String> list = new ArrayList<String>();
		        long count = 0;
		        int rowCount = 1;
				int limitCount = 1;
		        try {
		            while (driver.getResults(list)) {
		                for (String r : list) {
		                	if(rowCount > (page-1)*limit && limitCount <= limit) {
		                		datas.add(r);
				                count++;
				                limitCount++;
		                	}
		                	rowCount++;  
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
			}
			catch(Exception e){
				resultStatus.setStatus(MessageInfo.ERROR);
				resultStatus.setException(printStackTrace(e));
				return resultStatus;
			} finally {
				if(resultFilePath != null) {
					super.writeFileToHdfs(resultStatus, resultFilePath, this.fileName, false);
				}
			}
			return resultStatus;
	}
	
	private void initHiveConf(){
		this.hiveConf = new HiveConf();
		hiveConf.addResource(this.connection.getConfig());

		hiveConf.set("hive.dbname", "default");
		if (this.hiveMetaConnectionBean.getHiveConnType().equals(HiveMetaConnectionBean.EnumHiveMetaStoreConnectType.LOCAL)) {
			hiveConf.set("javax.jdo.option.ConnectionURL", this.hiveMetaConnectionBean.getMetaStoreConnectURL());
			hiveConf.set("javax.jdo.option.ConnectionDriverName", this.hiveMetaConnectionBean.getMetaStoreConnectDriver());
			hiveConf.set("javax.jdo.option.ConnectionUserName", this.hiveMetaConnectionBean.getMetaStoreConnectUserName());
			hiveConf.set("javax.jdo.option.ConnectionPassword", this.hiveMetaConnectionBean.getMetaStoreConnectPassword());
		} else if (this.hiveMetaConnectionBean.getHiveConnType().equals(HiveMetaConnectionBean.EnumHiveMetaStoreConnectType.REMOTE)) {
			hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, this.hiveMetaConnectionBean.getMetaUris());
			hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, false);
		} else if (this.hiveMetaConnectionBean.getHiveConnType().equals(HiveMetaConnectionBean.EnumHiveMetaStoreConnectType.SERVER2)){
			hiveConf.set(HiveMetaConnectionBean.HIVESERVER2URL, this.hiveMetaConnectionBean.getHiveServer2Url());
		} else {
			hiveConf.setBoolean(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS.varname, true);
			hiveConf.set("hive.metastore.authorization.storage.checks", "false");
			hiveConf.set("hive.metastore.warehouse.dir", this.hiveMetaConnectionBean.getEmbedPath() + "/hive/warehouse/");
			hiveConf.set("hive.exec.local.scratchdir", this.hiveMetaConnectionBean.getEmbedPath() + "/hive/tmp/hive");
			hiveConf.set("hive.querylog.location", this.hiveMetaConnectionBean.getEmbedPath() + "/hive/tmp/user/");
			hiveConf.set("hive.log.dir", this.hiveMetaConnectionBean.getEmbedPath() + "/hive/tmp/user/");
		}
		
		/* for join use hive */
		hiveConf.set("hadoop.bin.path",System.getProperty("user.home")+"/.haredb/exec/hadoop/bin/hadoop");
		/* **** */
		
		hiveConf.set("hbase.rpc.timeout", "9999999");
		hiveConf.set("hbase.client.retries.number", "1");
		hiveConf.set(HareQLConfiguration.SECONDRESULTBEANSPLITSIZE, "500");
		hiveConf.set(HareQLConfiguration.FIRSTRESULTBEANSPLITSIZE, "1000");
	}
}
