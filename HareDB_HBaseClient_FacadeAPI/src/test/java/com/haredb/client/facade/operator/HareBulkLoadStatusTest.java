package com.haredb.client.facade.operator;

import org.junit.Test;

import com.haredb.client.facade.bean.BulkloadStatusBean;
import com.haredb.hbaseclient.core.Connection;

public class HareBulkLoadStatusTest {
	
	@Test
	public void testgetBulkLoadStatus(){
		HareBulkLoadStatus bulkloadStatus = new HareBulkLoadStatus(this.getConnection());
		BulkloadStatusBean bulkloadStatusBean = bulkloadStatus.getBulkLoadStatus("jobID000000001");
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
