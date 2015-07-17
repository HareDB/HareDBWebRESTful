package com.haredb.client.facade.operator;


import org.junit.Before;
import org.junit.Test;

import com.haredb.client.facade.bean.HareQLResultStatusBean;
import com.haredb.hbaseclient.core.Connection;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumHiveMetaStoreConnectType;

public class HareQueryHareQLTest {

	
	private Connection connection;
	private HiveMetaConnectionBean hiveMetaConnectionBean;
	private HareQueryHareQL hareQL;
	
	private String tempFilePath="temp/stana_host_c";
	private String sqlStatement="select * from stana_host_c";
	
	
	@Before
	public void initSetup(){
		connection = new Connection("host1", "2181");
		connection.setNameNodeHostPort("hdfs://host1:8020");
		connection.setRmAddressHostPort("host1:8032");
		connection.setRmSchedulerAddressHostPort("host1:8030");
		connection.setRmResourceTrackerAddressHostPort("host1:8031");
		connection.setRmAdminAddressHostPort("host1:8033");
		connection.setYarnNodeManagerAuxServices("mapreduce_shuffle");
		connection.setMrJobhistoryAddress("host1:10020");
		connection.create();
		
		hiveMetaConnectionBean = new HiveMetaConnectionBean();
		hiveMetaConnectionBean.setHiveConnType(EnumHiveMetaStoreConnectType.LOCAL);
		hiveMetaConnectionBean.setMetaStoreConnectDriver("com.mysql.jdbc.Driver");
		hiveMetaConnectionBean.setMetaStoreConnectURL("jdbc:mysql://192.168.1.214:3306/hare");
		hiveMetaConnectionBean.setMetaStoreConnectUserName("root");
		hiveMetaConnectionBean.setMetaStoreConnectPassword("123456");
		
		
		hareQL = new HareQueryHareQL(connection, hiveMetaConnectionBean);
	}
	
	@Test
	public void testexecuteHareQL(){
		int page = 10;
		int limit = 10;
		HareQLResultStatusBean status;
		try {
			status = hareQL.executeHareQL(tempFilePath, sqlStatement, page, limit);
			System.out.println(status.getResponseTime());
			System.out.println(status.getRowSize());
			System.out.println(status.getHeads());
			System.out.println(status.getResults());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	@Test
	public void testsubmitHareQL() {
		int page = 10;
		int limit = 10;
		HareQLResultStatusBean status;
		try {
			status = hareQL.submitHareQL(tempFilePath, sqlStatement, page, limit);
			System.out.println(status.getResponseTime());
			System.out.println(status.getRowSize());
			System.out.println(status.getHeads());
			System.out.println(status.getResults());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testgetHareQLStatus() {
		HareQLResultStatusBean status = hareQL.getHareQLStatus(tempFilePath);
		System.out.println(status.getStatus()+", and exception ="+status.getException());
	}
	
}
