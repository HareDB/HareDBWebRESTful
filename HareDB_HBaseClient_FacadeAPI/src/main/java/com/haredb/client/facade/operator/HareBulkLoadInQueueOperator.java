package com.haredb.client.facade.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.haredb.adapter.bean.BulkColumn;
import com.haredb.adapter.bean.BulkFileBean;
import com.haredb.adapter.bean.BulkTableBean;
import com.haredb.adapter.bean.BulkloadBean;
import com.haredb.adapter.bean.ColumnFamilyBean;
import com.haredb.adapter.bean.CommonBean.EnumErrorType;
import com.haredb.adapter.bean.ErrorBean;
import com.haredb.adapter.bulkload.BulkloadOperator;
import com.haredb.client.facade.bean.BulkloadStatusBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.UploadSchemaBean;
import com.haredb.client.facade.until.HareEnv;
import com.haredb.hbase.bulkload.JobDriver;
import com.haredb.hbase.bulkload.domain.BulkTable.PreSplitAlgo;
import com.haredb.hbaseclient.core.Connection;

public class HareBulkLoadInQueueOperator extends HareContrivance {
	
	private Properties properties=new Properties();
	private Properties bulkloadProp = new Properties();
	private Properties indexProp = new Properties();
	final UploadSchemaBean uploadSchemaBean;
	private String containerRealPath;
	private String jobName = null;
	
	private static Logger logger = Logger.getLogger(HareBulkLoadInQueueOperator.class);
	
	public HareBulkLoadInQueueOperator(Connection connection, UploadSchemaBean uploadSchemaBean){
		super(connection);
		this.uploadSchemaBean=uploadSchemaBean;
	}
	
	
	
	public BulkloadStatusBean runBulkloadInQueue() throws Exception{
		long timestamp = System.currentTimeMillis();
		long startTime = System.currentTimeMillis();
		this.jobName = "application_" + timestamp;
		BulkloadStatusBean rBean = new BulkloadStatusBean();
		
		logger.info("start to run bulkload in queue.");
		
		try {
			this.loadSchemaProperties(connection);
			connection.getConfig().set(JobDriver.BULK_LOG_DEFAULT_ROOT_PATH, "/tmp");
		} catch (Exception e) {
			logger.error("Excption when load schema properties.");
			MessageInfo info = new MessageInfo();
			info.setStatus(MessageInfo.ERROR);
			info.setException(printStackTrace(e));
			writeFileToHdfs(info, uploadSchemaBean.getResultPath(),true);
		}
		rBean.setJobName(this.jobName);
		
		
		Thread bkLoadThread = new Thread(new Runnable() {
			@Override
			public void run() {
				BulkloadOperator operator = new BulkloadOperator(connection);
				try {
					ErrorBean errorBean = operator.bulkload(getBulkloadFileBean(), getBulkTableBean(), getBulkloadBean());
					
					if(errorBean.getErrorType().equals(EnumErrorType.ERROR)){
						throw new Exception(errorBean.getErrorObj()); 
					}
				}catch(Exception e) {
					/* for error  */
					MessageInfo info = new MessageInfo();
					info.setStatus(MessageInfo.ERROR);
					info.setException(printStackTrace(e));
					try {
						writeFileToHdfs(info, uploadSchemaBean.getResultPath(),true);
					} catch (Exception e1) {
						throw new RuntimeException(e1);
					}		
				}	
			}
		});
		bkLoadThread.start();
		
		
		rBean.setStatus(MessageInfo.SUCCESS);
		long endTime = System.currentTimeMillis();
		rBean.setResponseTime(endTime - startTime);
		return rBean;
	}
	private BulkTableBean getBulkTableBean(){
		BulkTableBean bulkTableBean = new BulkTableBean();
		
		bulkTableBean.setCompression(this.bulkloadProp.getProperty("bulkloadcompression"));
		bulkTableBean.setPreSplit(false);
		bulkTableBean.setStartKey(this.bulkloadProp.getProperty("bulkloadStartkey"));
		bulkTableBean.setEndKey(this.bulkloadProp.getProperty("bulkloadStopkey"));
		bulkTableBean.setRowKeyRange("");
		bulkTableBean.setPreSplitAlgo(PreSplitAlgo.NONE.toString());
		bulkTableBean.setRegionCount(1);
		bulkTableBean.setTableName(this.properties.getProperty("bulkloadtablename"));
		String columnFamilyProp = this.properties.getProperty("bulkloadcolumnfamily");
		if(columnFamilyProp != null){
			String columnFamilys[] = columnFamilyProp.split(",");
			List<ColumnFamilyBean> cfList = new ArrayList<ColumnFamilyBean>();
			for(String columnFamily : columnFamilys){
				cfList.add(new ColumnFamilyBean(columnFamily));
			}
			bulkTableBean.setCfList(cfList);
		}
		
		return bulkTableBean;
	}
	
	
	private BulkFileBean getBulkloadFileBean(){
		BulkFileBean bulkFileBean = new BulkFileBean();
		bulkFileBean.setFileLocation(this.bulkloadProp.getProperty("bulkloadfilelocalhdfs"));
		
		if(this.properties.getProperty("bulkloadexistheader").toLowerCase().equals("true")){
			bulkFileBean.setExistHeader(true);
		}else{
			bulkFileBean.setExistHeader(false);
		}
		bulkFileBean.setFileType(this.properties.getProperty("bulkloadfileformat"));
		bulkFileBean.setOutputFolder(this.properties.getProperty("bulkloadoutputfolder"));
		bulkFileBean.setSeparator(this.properties.getProperty("bulkloadseparator"));
		
		bulkFileBean.setInfoFilePath("/tmp");
		bulkFileBean.setLogFilePath("/tmp");
		bulkFileBean.setFilePath(this.uploadSchemaBean.getDataPath());
		
		if(this.uploadSchemaBean.getResultPath() == null || this.uploadSchemaBean.getResultPath().trim() == "") {
			throw new RuntimeException("Parameter is empty: resultPath");
		} else {
			bulkFileBean.setBkResultFilePath(this.uploadSchemaBean.getResultPath());
		}
		
		return bulkFileBean;
	}
	
	
	private BulkloadBean getBulkloadBean(){
		BulkloadBean bulkloadBean = new BulkloadBean();
		bulkloadBean.setJobName(jobName);
		bulkloadBean.setIndexJarPath(HareEnv.getIndexJar().getPath());
		bulkloadBean.setBulkloadType(this.bulkloadProp.getProperty("bulkloadtype"));
		bulkloadBean.setJarPath(HareEnv.getBulkloadJar().getPath());
		
		if(this.bulkloadProp.getProperty("bulkloadskipbadline").toLowerCase().equals("true")){
			bulkloadBean.setSkipBadLine(true);
		}else{
			bulkloadBean.setSkipBadLine(false);
		}
				
		String columnProps = this.properties.getProperty("bulkloadcolumn");
		String columnisRowkeyProps = this.properties.getProperty("bulkloadcolumnrowkey");
			
		if(columnProps != null && columnisRowkeyProps != null){
			String columnisRowkeySplits[] = columnisRowkeyProps.split(",");
			String columnSplits[] = columnProps.split(",");
			List<BulkColumn> columns = new ArrayList<BulkColumn>();
			int i = 0;
			for(String columnSplit:columnSplits){
				boolean isRowKey = false;
				if(columnisRowkeySplits[i].toLowerCase().equals("true")){
					isRowKey = true;
				}
				BulkColumn column = new BulkColumn();
				column.setKey(isRowKey);
				column.setName(columnSplit);
				columns.add(column);
				i++;
			}
			bulkloadBean.setColumns(columns);
		}else{
			throw new RuntimeException("Please input hbase column name and rowkey flag");
		}
		return bulkloadBean;
	}
	
	
	
	
	
	protected void loadSchemaProperties(Connection connection) throws Exception{
		Configuration config = connection.getConfig();
		FileSystem fs =null;
		FSDataInputStream inStream =null;
		try{
			fs = FileSystem.get(config);
			inStream = fs.open(new Path(this.uploadSchemaBean.getSchemaFilePath()));
			this.properties.load(inStream);
			this.bulkloadProp.load(HareBulkLoadInQueueOperator.class.getResourceAsStream("/bulkload.prop"));
			this.indexProp.load(HareBulkLoadInQueueOperator.class.getResourceAsStream("/index.prop"));
		}finally{
			inStream.close();
			fs.close();
		}
		
	}
	public String getContainerRealPath() {
		return containerRealPath;
	}
	public void setContainerRealPath(String containerRealPath) {
		this.containerRealPath = containerRealPath;
	}
	
	
	
	
	
}
