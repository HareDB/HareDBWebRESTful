package com.haredb.client.facade.operator;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.MetaColumnFamilyBean;
import com.haredb.client.facade.bean.TableInfoBean;
import com.haredb.hbaseclient.core.Connection;

public class HareDefineHTableTest {
	private static String zookeeperHostName = "host1";
	private static String zookeeperHostPort = "2181";
	@Test
	public void testCreateHTable(){
		Connection connection = new Connection(zookeeperHostName, zookeeperHostPort);
		
		HareDefineHTable createHTable = new HareDefineHTable(connection);
		TableInfoBean tableInfoBean = new TableInfoBean();
		tableInfoBean.setTableName("table82");
		tableInfoBean.setColumnFamilys(Arrays.asList("cf"));
		MessageInfo messageInfo = createHTable.createHTable(tableInfoBean);
		assertEquals("success", messageInfo.getStatus());
	}
	@Test
	public void testDropHTable(){
		Connection connection = new Connection(zookeeperHostName, zookeeperHostPort);
		HareDefineHTable dropHTable = new HareDefineHTable(connection);
		MessageInfo messageInfo = dropHTable.dropHTable("table82");
		assertEquals("success", messageInfo.getStatus());
	}

	@Test
	public void testAlterHTable(){
		Connection connection = new Connection(zookeeperHostName, zookeeperHostPort);
		HareDefineHTable alterHTable = new HareDefineHTable(connection);
		MetaColumnFamilyBean metaColumnFamilyBean = new MetaColumnFamilyBean();
		metaColumnFamilyBean.setColumnFamilyName("cf");
		metaColumnFamilyBean.setTtl(12345);
		MessageInfo messageInfo = alterHTable.alterHTable("table83", metaColumnFamilyBean);
		assertEquals("success", messageInfo.getStatus());
	}
}
