package com.haredb.client.facade.operator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.haredb.client.facade.bean.DataCellBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.hbaseclient.core.Connection;

public class HareManipulationHTableTest {
	private static String zookeeperHostName = "host1";
	private static String zookeeperHostPort = "2181";
	
	@Test
	public void testhtablePut(){
		Connection connection = new Connection(zookeeperHostName, zookeeperHostPort);
		HareManipulationHTable putData = new HareManipulationHTable(connection);
		
		DataCellBean dataCellBean = new DataCellBean();
		dataCellBean.setTableName("table83");
		dataCellBean.setRowkey("rowkey1");
		dataCellBean.setColumnFamily("cf");
		dataCellBean.setQualifier("column1");
		dataCellBean.setValue("value1");
		MessageInfo messageInfo = putData.htablePut(dataCellBean);
		assertEquals("success", messageInfo.getStatus());
	}
	
	@Test
	public void testhtableDelete1(){
		Connection connection = new Connection(zookeeperHostName, zookeeperHostPort);
		HareManipulationHTable deleteData = new HareManipulationHTable(connection);
		MessageInfo messageInfo = deleteData.htableDelete("table83", "rowkey2");
		assertEquals("success", messageInfo.getStatus());
	}
	@Test
	public void testhtableDelete2(){
		Connection connection = new Connection(zookeeperHostName, zookeeperHostPort);
		HareManipulationHTable deleteData = new HareManipulationHTable(connection);
		MessageInfo messageInfo = deleteData.htableDelete("table83", "rowkey3", "cf", "column1");
		assertEquals("success", messageInfo.getStatus());
	}
	@Test
	public void testhtableDelete3(){
		Connection connection = new Connection(zookeeperHostName, zookeeperHostPort);
		HareManipulationHTable deleteData = new HareManipulationHTable(connection);
		MessageInfo messageInfo = deleteData.htableDelete("table83", "rowkey4", "cf");
		assertEquals("success", messageInfo.getStatus());
	}
	
	
}
