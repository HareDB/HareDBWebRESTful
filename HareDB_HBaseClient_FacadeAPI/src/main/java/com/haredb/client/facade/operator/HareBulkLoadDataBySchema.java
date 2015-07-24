package com.haredb.client.facade.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.haredb.client.facade.bean.BulkFileBean;
import com.haredb.client.facade.bean.BulkTableBean;
import com.haredb.client.facade.bean.BulkloadBean;
import com.haredb.client.facade.bean.BulkloadStatusBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.UploadSchemaBean;
import com.haredb.hbase.bulkload.domain.BulkColumn;
import com.haredb.hbase.bulkload.domain.BulkTable.PreSplitAlgo;
import com.haredb.hbaseclient.core.Connection;

public class HareBulkLoadDataBySchema extends HareContrivance{
	private Properties properties=new Properties();
	private Properties bulkloadProp = new Properties();
	private String containerRealPath;
	private String jobName = null;
	final UploadSchemaBean uploadSchemaBean;
		
	public HareBulkLoadDataBySchema(Connection connection, UploadSchemaBean uploadSchemaBean){
		super(connection);
		this.uploadSchemaBean=uploadSchemaBean;
	}
		
	public BulkloadStatusBean runSchemaBulkload() throws Exception{
		long timestamp = System.currentTimeMillis();
		long startTime = System.currentTimeMillis();
		this.jobName = "application_" + timestamp;
		BulkloadStatusBean rBean = new BulkloadStatusBean();
		try {
			this.loadSchemaProperties(connection);
		} catch (Exception e) {
			MessageInfo info = new MessageInfo();
			info.setStatus(MessageInfo.ERROR);
			info.setException(e.getMessage());
			writeFileToHdfs(info, uploadSchemaBean.getResultPath(),true);
		}
		rBean.setJobName(this.jobName);
		rBean.setStatus(MessageInfo.SUCCESS);
		Thread bkLoadThread = new Thread(new Runnable() {
			@Override
			public void run() {
				HareBulkLoadData bulkload = new HareBulkLoadData(connection);
				try {
					bulkload.runbulkload(getBulkloadBean(), getBulkTableBean(), getBulkFileBean());
				}catch(Exception e) {
					MessageInfo info = new MessageInfo();
					info.setStatus(MessageInfo.ERROR);
					info.setException(e.getMessage());
					try {
						bulkload.writeFileToHdfs(info, uploadSchemaBean.getResultPath(),true);
					} catch (Exception e1) {
						throw new RuntimeException(e1);
					}
				}	
			}
			
		});
		bkLoadThread.start();
		long endTime = System.currentTimeMillis();
		rBean.setResponseTime(endTime - startTime);
		return rBean;
	}
	
	protected void loadSchemaProperties(Connection connection) throws Exception{
		Configuration config = connection.getConfig();
			
		FileSystem fs = FileSystem.get(config);
	    FSDataInputStream inStream = fs.open(new Path(this.uploadSchemaBean.getSchemaFilePath()));
			
		this.properties.load(inStream);
		this.bulkloadProp.load(HareBulkLoadDataBySchema.class.getResourceAsStream("/bulkload.prop"));
		
	}
	
	protected BulkloadBean getBulkloadBean(){
		BulkloadBean bulkLoadBean = new BulkloadBean();
		bulkLoadBean.setJobName(this.jobName);
		bulkLoadBean.setBulkloadType(this.bulkloadProp.getProperty("bulkloadtype"));
		if(this.containerRealPath == null) {
			throw new RuntimeException("Exception: Container Real Path is null!");
		}
		bulkLoadBean.setJarPath(this.containerRealPath+this.bulkloadProp.getProperty("bulkloadjarpath"));
		
		if(this.bulkloadProp.getProperty("bulkloadskipbadline").toLowerCase().equals("true")){
			bulkLoadBean.setSkipBadLine(true);
		}else{
			bulkLoadBean.setSkipBadLine(false);
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
				BulkColumn column = new BulkColumn(columnSplit, isRowKey);
				columns.add(column);
				i++;
			}
			bulkLoadBean.setColumns(columns);
		}else{
			throw new RuntimeException("Please input hbase column name and rowkey flag");
		}
		return bulkLoadBean;
	}
	
	protected BulkTableBean getBulkTableBean(){
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
			List<String> cfList = new ArrayList<String>();
			for(String columnFamily : columnFamilys){
				cfList.add(columnFamily);
			}
			bulkTableBean.setCfList(cfList);
		}
		
		return bulkTableBean;
	}
	
	protected BulkFileBean getBulkFileBean(){
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
		bulkFileBean.setBkResultFilePath(this.uploadSchemaBean.getResultPath());
		return bulkFileBean;
	}

	public UploadSchemaBean getUploadSchemaBean() {
		return uploadSchemaBean;
	}
	
	public void setContainerRealPath(String realPath){
		this.containerRealPath = realPath;
	}
}
