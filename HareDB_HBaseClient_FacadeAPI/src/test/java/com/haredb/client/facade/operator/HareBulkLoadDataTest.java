package com.haredb.client.facade.operator;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.haredb.client.facade.bean.BulkFileBean;
import com.haredb.client.facade.bean.BulkTableBean;
import com.haredb.client.facade.bean.BulkloadBean;
import com.haredb.hbase.bulkload.domain.BulkColumn;
import com.haredb.hbase.bulkload.domain.BulkTable.PreSplitAlgo;
import com.haredb.hbaseclient.core.Connection;

public class HareBulkLoadDataTest {

	
	@Test
	public void testrunbulkload(){
		HareBulkLoadData bulkload = new HareBulkLoadData(this.getConnection());
		String jobName = bulkload.runbulkload(this.getBulkloadBean(), this.getBulkTableBean(), this.getBulkFileBean());
		System.out.println(jobName);
	}
	private BulkloadBean getBulkloadBean(){
		BulkloadBean bulkLoadBean = new BulkloadBean();
		bulkLoadBean.setJobName("job000000002");
		bulkLoadBean.setBulkloadType(HareBulkLoadData.BULK_TYPE_HFILE);
		bulkLoadBean.setJarPath("/home/user1/bulkload.jar");
		
		List<BulkColumn> columns = new ArrayList<BulkColumn>();
		BulkColumn bk1 = new BulkColumn(":key", true);
		columns.add(bk1);
		
		BulkColumn bk2 = new BulkColumn("cf:column5", false);
		columns.add(bk2);
		
		BulkColumn bk3 = new BulkColumn("cf:column6", false);
		columns.add(bk3);
		
		bulkLoadBean.setColumns(columns);
		return bulkLoadBean;
	}
	
	private BulkTableBean getBulkTableBean(){
		BulkTableBean bulkTableBean = new BulkTableBean();
		bulkTableBean.setCompression("false");
		bulkTableBean.setPreSplit(false);
		bulkTableBean.setStartKey("");
		bulkTableBean.setEndKey("");
		bulkTableBean.setRowKeyRange("");
		bulkTableBean.setPreSplitAlgo(PreSplitAlgo.NONE.toString());
		bulkTableBean.setRegionCount(1);
		
		List<String> cfList = new ArrayList<String>();
		cfList.add("cf");
		bulkTableBean.setCfList(cfList);
		
		bulkTableBean.setTableName("table100");
		return bulkTableBean;
	}
	
	private BulkFileBean getBulkFileBean(){
		BulkFileBean bulkFileBean = new BulkFileBean();
		bulkFileBean.setExistHeader(false);
		bulkFileBean.setFileLocation(HareBulkLoadData.BULK_FILE_LOCATION_HDFS);
		bulkFileBean.setFileType(HareBulkLoadData.BULK_FILE_TYPE_NORMAL);
		bulkFileBean.setInfoFilePath("/tmp");
		bulkFileBean.setLogFilePath("/tmp");
		bulkFileBean.setFilePath("/aaa.txt");
		bulkFileBean.setOutputFolder("hdfs://host1:9000/result");
		bulkFileBean.setSeparator("|");
		return bulkFileBean;
	}
	
	private Connection getConnection(){
		Connection connection = new Connection("host1", "2181");
		connection.setNameNodeHostPort("hdfs://host1:9000");
		connection.setRmAddressHostPort("host1:8032");
		connection.setRmSchedulerAddressHostPort("host1:8030");
		connection.setRmResourceTrackerAddressHostPort("host1:8031");
		connection.setRmAdminAddressHostPort("host1:8033");
		connection.setYarnNodeManagerAuxServices("mapreduce_shuffle");
		connection.setMrJobhistoryAddress("host1:10020");
		return connection;
	}
}
