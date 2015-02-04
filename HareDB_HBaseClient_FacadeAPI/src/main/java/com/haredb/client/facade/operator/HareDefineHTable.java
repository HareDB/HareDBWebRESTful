package com.haredb.client.facade.operator;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;

import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.MetaColumnFamilyBean;
import com.haredb.client.facade.bean.TableInfoBean;
import com.haredb.hbaseclient.core.Connection;
import com.haredb.hbaseclient.core.HareTable;
import com.haredb.hbaseclient.core.MetaColumnFamily;

public class HareDefineHTable extends HareContrivance{
	private Connection connection;
	
	
	public HareDefineHTable(Connection connection){
		super(connection);
		this.connection = connection;
		this.connection.create();
	}
	

	public MessageInfo createHTable(TableInfoBean tableInfoBean){
		MessageInfo messageInfo = new MessageInfo();
		String coprocessorJarPath;
		try{
			if(super.checkNull(tableInfoBean.getTableName())) {
				throw new RuntimeException("table name is null!");
			} 
			if(super.checkNull(tableInfoBean.getColumnFamilys())) {
				throw new RuntimeException("column families are null!");
			}
			long startTime = System.currentTimeMillis();
			HareTable hareTable = new HareTable(connection);
			hareTable.setName(tableInfoBean.getTableName());
			for(String columnFamily : tableInfoBean.getColumnFamilys()){
				MetaColumnFamily metaColumnFamily = new MetaColumnFamily(columnFamily);
				hareTable.addColumnFamilies(metaColumnFamily);				
			}
			hareTable.create();
			//register coprocessor
			coprocessorJarPath = this.connection.getNameNodeHostPort()+this.getCoprocessorProp().getProperty("jarpath");
			hareTable.addCoprocessor(hareTable.HAREDB_COPROCESSOR_ENDPOINT, coprocessorJarPath);
			long endTime = System.currentTimeMillis();
			messageInfo.setStatus(MessageInfo.SUCCESS);
			messageInfo.setResponseTime(endTime - startTime);
		}catch(TableExistsException e) {
			messageInfo.setStatus(MessageInfo.ERROR);
			messageInfo.setException(tableInfoBean.getTableName()+" has already exist!");
			return messageInfo;
		}catch(Exception e){
			messageInfo.setStatus(MessageInfo.ERROR);
			messageInfo.setException(e.getMessage());
			return messageInfo;
		}
		return messageInfo;
	}
	
	public MessageInfo dropHTable(String tableName){
		MessageInfo messageInfo = new MessageInfo();
		try{
			long startTime = System.currentTimeMillis();
			HareTable hareTable = new HareTable(connection);
			hareTable.setName(tableName);
			hareTable.dropHTable();
			long stopTime = System.currentTimeMillis();
			messageInfo.setStatus(MessageInfo.SUCCESS);
			messageInfo.setResponseTime(stopTime - startTime);
		}catch(Exception e){
			if(e.getCause().getClass().equals(TableNotFoundException.class)) {
				messageInfo.setStatus(MessageInfo.ERROR);
				messageInfo.setException(tableName+" not found!");
			} else {
				messageInfo.setStatus(MessageInfo.ERROR);
				messageInfo.setException(ExceptionUtils.getRootCauseMessage(e.getCause()));
			}
			return messageInfo;
		}
			return messageInfo; 
	}

	public MessageInfo alterHTable(String tableName, MetaColumnFamilyBean metaColumnFamilyBean){
		MessageInfo messageInfo = new MessageInfo();
		try{
			long startTime = System.currentTimeMillis();
			HareTable hareTable = new HareTable(connection, tableName);
			MetaColumnFamily metaColumnFamily = this.getMetaColumnFamily(metaColumnFamilyBean);
			hareTable.updateColumnFamily(metaColumnFamily);
			long stopTime = System.currentTimeMillis();
			messageInfo.setStatus(MessageInfo.SUCCESS);
			messageInfo.setResponseTime(stopTime - startTime);
			
		}catch(Exception e){
			messageInfo.setStatus(MessageInfo.ERROR);
			messageInfo.setException(e.getMessage());
			return messageInfo;
		}
		return messageInfo;
		
	}
	
	
	/**
	 * 取得coprocessor.prop資訊檔
	 * @return
	 * @throws IOException
	 */
	private Properties getCoprocessorProp() throws IOException{
		Properties prop = new Properties();
		prop.load(HareDefineHTable.class.getResourceAsStream("/coprocessor.prop"));
		return prop;
	};
	
	private MetaColumnFamily getMetaColumnFamily(MetaColumnFamilyBean metaColumnFamilyBean){
		MetaColumnFamily metaColumnFamily = new MetaColumnFamily(metaColumnFamilyBean.getName());
		metaColumnFamily.setColumnFamilyName(metaColumnFamilyBean.getName());
		metaColumnFamily.setDataBlockEncoding(metaColumnFamilyBean.getDataBlockEncoding());
		metaColumnFamily.setBloomFilter(metaColumnFamilyBean.getBloomFilter());
		metaColumnFamily.setReplicationScope(metaColumnFamilyBean.getReplicationScope());
		metaColumnFamily.setVersions(metaColumnFamilyBean.getVersions());
		metaColumnFamily.setCompression(metaColumnFamilyBean.getCompression());
		metaColumnFamily.setMinVersions(metaColumnFamilyBean.getMinVersions());
		metaColumnFamily.setTtl(metaColumnFamilyBean.getTtl());
		metaColumnFamily.setBlocksize(metaColumnFamilyBean.getBlocksize());
		metaColumnFamily.setInMemory(metaColumnFamilyBean.isInMemory());
		metaColumnFamily.setEncodeOnDisk(metaColumnFamilyBean.isEncodeOnDisk());
		metaColumnFamily.setBlockcache(metaColumnFamilyBean.isBlockcache());
		metaColumnFamily.setVersions(metaColumnFamilyBean.getVersions());
		return metaColumnFamily;
	}
}
