package com.haredb.client.facade.operator;


import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import com.haredb.client.facade.bean.HareQLResultStatusBean;
import com.haredb.hbaseclient.core.Connection;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumHiveMetaStoreConnectType;
import com.haredb.hive.ql.HareDriver;

public class HareQueryHareQLTest {

	
	private Connection connection;
	private HiveMetaConnectionBean hiveMetaConnectionBean;
	private HareQueryHareQL hareQL;
	
	private String tempFilePath="temp/stana_host_c";
	private String sqlStatement="select * from stana_host_c";
	
//	@Before
//	@Test
	public void test() throws Exception{
		 HiveConf hiveConf = new HiveConf();
	        hiveConf.set("hbase.zookeeper.quorum", "host1");
	        hiveConf.set("hbase.zookeeper.property.clientPort", "2181");
	        hiveConf.set("fs.default.name", "hdfs://host1:8020");
	        hiveConf.set("yarn.resourcemanager.address", "host1:8032");
	        hiveConf.set("yarn.resourcemanager.scheduler.address", "host1:8030");
	        hiveConf.set("yarn.resourcemanager.resource-tracker.address", "host1:8031");
	        hiveConf.set("yarn.resourcemanager.admin.address", "host1:8033");
	        hiveConf.set("mapreduce.framework.name", "yarn");
	        hiveConf.set("mapreduce.johistory.address", "host1:10020");
	        hiveConf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
	        hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:mysql://192.168.1.214:3306/hare");
	        hiveConf.set("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver");
	        hiveConf.set("javax.jdo.option.ConnectionUserName", "root");
	        hiveConf.set("javax.jdo.option.ConnectionPassword", "123456");
	        
	        
	        hiveConf.set("hive.dbname", "default");
	        hiveConf.set("hbase.client.retries.number", "1");
	        hiveConf.set("hbase.rpc.timeout", "9999999");
	        hiveConf.set("cluster.kerberos.enabled","false");
	        
	        CliSessionState ss = new CliSessionState(hiveConf);
	        UserGroupInformation.setConfiguration(hiveConf);
	        SessionState.start(ss);
	      
	        HareDriver driver = new HareDriver(hiveConf);
	        CommandProcessorResponse res = driver.run(sqlStatement);

	    	System.out.println("Response Code:" + res.getResponseCode());
	        System.out.println("Error Message:" + res.getErrorMessage());
	        System.out.println("SQL State:" + res.getSQLState());
	       
	       Schema s = res.getSchema();
	       // System.out.println(s.getFieldSchemas());
	       if (s != null) {
	            List<FieldSchema> schema = s.getFieldSchemas();
	            if ((schema != null) && (!schema.isEmpty())) {
	                for (int pos = 0; pos < schema.size(); pos++) {
	                    //System.out.println("Name:" + schema.get(pos).getName() + ", Type:" + schema.get(pos).getType());
	                    				//System.out.println(schema.get(pos).getName());
	                }
	            }
	       }
	       int count = 0;
	        ArrayList<String> list = new ArrayList<String>();
	        try {
	            while (driver.getResults(list)) {
	                for (String r : list) {
	                   System.out.println(r);
	                   count++;
	                }
	                list.clear();
	            }
	            System.out.println("COUNT:" + count);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        ss.close();
	}
	
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
