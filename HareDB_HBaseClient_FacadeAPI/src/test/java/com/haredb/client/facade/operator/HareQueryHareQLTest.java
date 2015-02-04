package com.haredb.client.facade.operator;

import org.junit.Test;

import com.haredb.client.facade.bean.HareQLResultStatusBean;
import com.haredb.hbaseclient.core.Connection;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumHiveMetaStoreConnectType;

public class HareQueryHareQLTest {

	@Test
	public void testexecuteHareQL(){
		
		Connection connection = this.getConnection();
		HiveMetaConnectionBean hiveMetaConnectionBean = this.getHiveMetaConnectionBean();
		

		HareQueryHareQL hareQL = new HareQueryHareQL(connection, hiveMetaConnectionBean);
		String sql = "SELECT * FROM table81";
		int page = 10;
		int limit = 10;
		HareQLResultStatusBean status = hareQL.executeHareQL(null, sql, page, limit);
		System.out.println(status.getResponseTime());
		System.out.println(status.getRowSize());
		System.out.println(status.getHeads());
		System.out.println(status.getResults());
		
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
	private HiveMetaConnectionBean getHiveMetaConnectionBean(){
		HiveMetaConnectionBean hiveMetaConnectionBean = new HiveMetaConnectionBean();
		
		EnumHiveMetaStoreConnectType connectionTypeEnum = EnumHiveMetaStoreConnectType.LOCAL;
		hiveMetaConnectionBean.setHiveConnType(connectionTypeEnum);
		hiveMetaConnectionBean.setMetaStoreConnectDriver("com.mysql.jdbc.Driver");
		hiveMetaConnectionBean.setMetaStoreConnectURL("jdbc:mysql://192.168.1.215:3306/metastore_db");
		hiveMetaConnectionBean.setMetaStoreConnectUserName("root");
		hiveMetaConnectionBean.setMetaStoreConnectPassword("123456");
		return hiveMetaConnectionBean;
	}
}
