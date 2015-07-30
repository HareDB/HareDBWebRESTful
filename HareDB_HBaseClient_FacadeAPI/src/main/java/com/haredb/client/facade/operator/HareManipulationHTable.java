package com.haredb.client.facade.operator;

import com.haredb.client.facade.bean.DataCellBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.hbaseclient.core.Connection;
import com.haredb.hbaseclient.core.HTableAccessOperator;

public class HareManipulationHTable extends HareContrivance{
	
	public HareManipulationHTable(Connection connection){
		super(connection);
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
		
			HTableAccessOperator.putRecord(this.connection, tableName, rowkey, columnFamily, qualifierNames, value);
			long stopTime = System.currentTimeMillis();
			messageBean.setStatus(MessageInfo.SUCCESS);
			messageBean.setResponseTime(stopTime - startTime);
		}catch(Exception e){
			messageBean.setStatus(MessageInfo.ERROR);
			messageBean.setException(printStackTrace(e));
			return messageBean;
		}
		return messageBean;
	}
	
	public MessageInfo htableDelete(String tableName, String rowkey, String columnFamily, String qualifier){
		MessageInfo messageBean = new MessageInfo();
		try{
			long startTime = System.currentTimeMillis();
			HTableAccessOperator.deleteRecord(this.connection, tableName, rowkey, columnFamily, qualifier);
			long stopTime = System.currentTimeMillis();
			messageBean.setStatus(MessageInfo.SUCCESS);
			messageBean.setResponseTime(stopTime - startTime);
		}catch(Exception e){
			messageBean.setStatus(MessageInfo.ERROR);
			messageBean.setException(printStackTrace(e));
			return messageBean;
		}
		return messageBean;
		
	}
	
	public MessageInfo htableDelete(String tableName, String rowkey, String columnFamily){
		MessageInfo messageBean = new MessageInfo();
		try{
			long startTime = System.currentTimeMillis();
			HTableAccessOperator.deleteOneRowRecord(this.connection, tableName, columnFamily, rowkey);
			long stopTime = System.currentTimeMillis();
			messageBean.setStatus(MessageInfo.SUCCESS);
			messageBean.setResponseTime(stopTime - startTime);
		}catch(Exception e){
			messageBean.setStatus(MessageInfo.ERROR);
			messageBean.setException(printStackTrace(e));
			return messageBean;
		}
		return messageBean;
		
	}
	
	public MessageInfo htableDelete(String tableName, String rowkey){
		MessageInfo messageBean = new MessageInfo();
		try{
			long startTime = System.currentTimeMillis();
			HTableAccessOperator.deleteRowkey(this.connection, tableName, rowkey);
			long stopTime = System.currentTimeMillis();
			messageBean.setStatus(MessageInfo.SUCCESS);
			messageBean.setResponseTime(stopTime -startTime);
		}catch(Exception e){
			messageBean.setStatus(MessageInfo.ERROR);
			messageBean.setException(printStackTrace(e));
			return messageBean;
		}
		return messageBean;
		
	}
	
}
