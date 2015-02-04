package com.haredb.client.facade.operator;

import com.haredb.client.facade.bean.DataCellBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.hbaseclient.core.Connection;
import com.haredb.hbaseclient.core.HTableAccessOperator;

public class HareManipulationHTable {
	private Connection connection;
	
	public HareManipulationHTable(Connection connection){
		this.connection = connection;
	}
	
	public MessageInfo htablePut(DataCellBean dataCellBean){
		MessageInfo messageBean = new MessageInfo();
		try{
			long startTime = System.currentTimeMillis();
			String tableName = dataCellBean.getTableName();
			String rowkey = dataCellBean.getRowkey();
			String columnFamily = dataCellBean.getColumnFamily();
			String qualifierNames = dataCellBean.getQualifier();
			String value = dataCellBean.getValue();
		
			HTableAccessOperator accessOperator = new HTableAccessOperator();
			accessOperator.putRecord(this.connection, tableName, rowkey, columnFamily, qualifierNames, value);
			long stopTime = System.currentTimeMillis();
			messageBean.setStatus(MessageInfo.SUCCESS);
			messageBean.setResponseTime(stopTime - startTime);
		}catch(Exception e){
			messageBean.setStatus(MessageInfo.ERROR);
			messageBean.setException(e.getMessage());
			throw new RuntimeException("Exception:", e);
		}finally{
			return messageBean;
		}
	}
	
	public MessageInfo htableDelete(String tableName, String rowkey, String columnFamily, String qualifier){
		MessageInfo messageBean = new MessageInfo();
		try{
			long startTime = System.currentTimeMillis();
			HTableAccessOperator accessOperator = new HTableAccessOperator();
			accessOperator.deleteRecord(this.connection, tableName, rowkey, columnFamily, qualifier);
			long stopTime = System.currentTimeMillis();
			messageBean.setStatus(MessageInfo.SUCCESS);
			messageBean.setResponseTime(stopTime - startTime);
		}catch(Exception e){
			messageBean.setStatus(MessageInfo.ERROR);
			messageBean.setException(e.getMessage());
			throw new RuntimeException("Exception:", e);
		}finally{
			return messageBean;
		}
	}
	
	public MessageInfo htableDelete(String tableName, String rowkey, String columnFamily){
		MessageInfo messageBean = new MessageInfo();
		try{
			long startTime = System.currentTimeMillis();
			HTableAccessOperator accessOperator = new HTableAccessOperator();
			accessOperator.deleteOneRowRecord(this.connection, tableName, columnFamily, rowkey);
			long stopTime = System.currentTimeMillis();
			messageBean.setStatus(MessageInfo.SUCCESS);
			messageBean.setResponseTime(stopTime - startTime);
		}catch(Exception e){
			messageBean.setStatus(MessageInfo.ERROR);
			messageBean.setException(e.getMessage());
			throw new RuntimeException("Exception:", e);
		}finally{
			return messageBean;
		}
	}
	
	public MessageInfo htableDelete(String tableName, String rowkey){
		MessageInfo messageBean = new MessageInfo();
		try{
			long startTime = System.currentTimeMillis();
			HTableAccessOperator accessOperator = new HTableAccessOperator();
			accessOperator.deleteRowkey(this.connection, tableName, rowkey);
			long stopTime = System.currentTimeMillis();
			messageBean.setStatus(MessageInfo.SUCCESS);
			messageBean.setResponseTime(stopTime -startTime);
		}catch(Exception e){
			messageBean.setStatus(MessageInfo.ERROR);
			messageBean.setException(e.getMessage());
			throw new RuntimeException("Exception:", e);
		}finally{
			return messageBean;
		}
	}
	
}
