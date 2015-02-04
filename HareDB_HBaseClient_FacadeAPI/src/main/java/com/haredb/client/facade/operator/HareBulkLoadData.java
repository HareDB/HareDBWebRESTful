package com.haredb.client.facade.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import com.haredb.client.facade.bean.BulkFileBean;
import com.haredb.client.facade.bean.BulkTableBean;
import com.haredb.client.facade.bean.BulkloadBean;
import com.haredb.hbase.bulkload.BulkloadInvoker;
import com.haredb.hbase.bulkload.bean.BulkInfoBean;
import com.haredb.hbase.bulkload.bean.FileInfoBean;
import com.haredb.hbase.bulkload.bean.TableInfoBean;
import com.haredb.hbase.bulkload.domain.BulkColumn;
import com.haredb.hbase.bulkload.domain.BulkFile.BulkFileLocation;
import com.haredb.hbase.bulkload.domain.BulkFile.BulkFileType;
import com.haredb.hbase.bulkload.domain.BulkTable.PreSplitAlgo;
import com.haredb.hbase.bulkload.domain.BulkloadDelivery.BulkloadType;
import com.haredb.hbaseclient.core.Connection;
import com.haredb.hbaseclient.core.HareTable;
import com.haredb.hbaseclient.core.MetaColumnFamily;


public class HareBulkLoadData extends HareContrivance{
	public final static String BULK_TYPE_HBASE = "HBASE";
	public final static String BULK_TYPE_HFILE = "HFile";
	public final static String BULK_FILE_LOCATION_HDFS = "HDFS";
	public final static String BULK_FILE_LOCATION_LOCAL = "LOCAL";
	public final static String BULK_FILE_TYPE_NORMAL = "NORMAL";
	public final static String BULK_FILE_TYPE_CSV = "CSV";
	
	public HareBulkLoadData(Connection connection){
		super(connection);
	}
	
	public String runbulkload(BulkloadBean bulkloadBean, BulkTableBean bulkTableBean, BulkFileBean bulkFileBean){
		BulkInfoBean bulkInfoBean = new BulkInfoBean();
		bulkInfoBean.setJobName(bulkloadBean.getJobName());
		if(bulkloadBean.getBulkloadType().equals(BULK_TYPE_HBASE)){
			bulkInfoBean.setBulkloadType(BulkloadType.HBASE);
		}else if(bulkloadBean.getBulkloadType().equals(BULK_TYPE_HFILE)){
			bulkInfoBean.setBulkloadType(BulkloadType.HFILE);
		}else{
			throw new RuntimeException("Bulkload type not support: " + bulkloadBean.getBulkloadType());
		}
		bulkInfoBean.setJarPath(bulkloadBean.getJarPath());
		bulkInfoBean.setSkipBadLine(bulkloadBean.isSkipBadLine());
		bulkInfoBean.setTimestamp(bulkloadBean.getTimestamp());
	
		//組合要Bulkload的欄位
		List<BulkColumn> bulkColumns = bulkloadBean.getColumns();
		bulkInfoBean.setColumns(bulkColumns);
		

		TableInfoBean tableInfoBean = new TableInfoBean();
		super.connection.create();
		try {
			//取得ColumnFamilyconnection.create();
			
			HareTable hareTable = new HareTable(super.connection, bulkTableBean.getTableName());
			if(hareTable.exists()){	
				List<MetaColumnFamily> columnFamilys = hareTable.getColumnFamilyInfo();
				tableInfoBean.setCfList(columnFamilys);
			}else{
				List<MetaColumnFamily> columnFamilys = new ArrayList<MetaColumnFamily>();
				List<String> cfLists = bulkTableBean.getCfList();
				for(String cfList : cfLists){
					MetaColumnFamily columnFamily = new MetaColumnFamily(cfList);
					columnFamilys.add(columnFamily);
				}
				tableInfoBean.setCfList(columnFamilys);
			}
			
		} catch (Exception e) {
			throw new RuntimeException("Changing the columnFamily object : " + e.getMessage());
		}
		tableInfoBean.setCompression(bulkTableBean.getCompression().equals("true")? Algorithm.GZ:Algorithm.NONE);
		tableInfoBean.setPreSplit(bulkTableBean.isPreSplit());
		tableInfoBean.setStartKey(bulkTableBean.getStartKey());
		tableInfoBean.setEndKey(bulkTableBean.getEndKey());
		tableInfoBean.setRowKeyRange(bulkTableBean.getRowKeyRange());
		if(bulkTableBean.getPreSplitAlgo().equals(PreSplitAlgo.NONE.toString())){
			tableInfoBean.setPreSplitAlgo(PreSplitAlgo.NONE);
		}else if(bulkTableBean.getPreSplitAlgo().equals(PreSplitAlgo.Default.toString())){
			tableInfoBean.setPreSplitAlgo(PreSplitAlgo.Default);
		}else if(bulkTableBean.getPreSplitAlgo().equals(PreSplitAlgo.Custom.toString())){
			tableInfoBean.setPreSplitAlgo(PreSplitAlgo.Custom);
		}else{
			throw new RuntimeException("PreSplit type not support");
		}
		tableInfoBean.setRegionCount(bulkTableBean.getRegionCount());
		tableInfoBean.setTableName(bulkTableBean.getTableName());
		tableInfoBean.setKeyDesignRule("");
		
		
		
		FileInfoBean fileInfoBean = new FileInfoBean();
		fileInfoBean.setExistHeader(bulkFileBean.isExistHeader());
		if(bulkFileBean.getFileLocation().equals(BULK_FILE_LOCATION_HDFS)){
			fileInfoBean.setFileLocation(BulkFileLocation.HDFS);
		}else if(bulkFileBean.getFileLocation().equals(BULK_FILE_LOCATION_LOCAL)){
			fileInfoBean.setFileLocation(BulkFileLocation.LOCAL);
		}else{
			throw new RuntimeException("Bulkload file location not support: " + bulkFileBean.getFileLocation());
		}
		
		if(bulkFileBean.getFileType().equals(BULK_FILE_TYPE_NORMAL)){
			fileInfoBean.setFileType(BulkFileType.NORMAL_TEXT);
		}else if(bulkFileBean.getFileType().equals(BULK_FILE_TYPE_CSV)){
			fileInfoBean.setFileType(BulkFileType.CSV);
		}else {
			throw new RuntimeException("Bulkload file type not support: " + bulkFileBean.getFileType());
		}
		
		fileInfoBean.setInfoFilePath(bulkFileBean.getInfoFilePath());
		fileInfoBean.setLogFilePath(bulkFileBean.getLogFilePath());
		fileInfoBean.setFilePath(bulkFileBean.getFilePath());
		fileInfoBean.setOutputFolder(bulkFileBean.getOutputFolder());
		fileInfoBean.setSeparator(bulkFileBean.getSeparator());
		if(bulkFileBean.getBkResultFilePath() == null || bulkFileBean.getBkResultFilePath().trim() == "") {
			throw new RuntimeException("Parameter is empty: resultPath");
		} else {
			fileInfoBean.setBkResultFilePath(bulkFileBean.getBkResultFilePath());
		}
		
		
		BulkloadInvoker bulkloadInvoker =  new BulkloadInvoker(super.connection);
		bulkloadInvoker.doBulkload(fileInfoBean, tableInfoBean, bulkInfoBean);
		return bulkInfoBean.getJobName();
	}
	
}