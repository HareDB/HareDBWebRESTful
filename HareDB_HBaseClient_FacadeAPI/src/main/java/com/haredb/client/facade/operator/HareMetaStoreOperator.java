package com.haredb.client.facade.operator;

import java.util.Date;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.MetaTableBean;
import com.haredb.hive.metastore.bean.TableBean;
import com.haredb.hive.metastore.bridge.MetaStoreBridge;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean;
import com.haredb.hive.metastore.factory.MetaStoreBridgeFactory;
import com.haredb.hive.metastore.service.HiveMetaStoreService;

public class HareMetaStoreOperator{

	private static Log LOG = LogFactory.getLog(HareMetaStoreOperator.class.getName());
	private MetaStoreBridge metaStoreBridge;
	private HiveMetaConnectionBean connectionBean;
	private long startTime;
	private long stopTime;
	
	public HareMetaStoreOperator(HiveMetaConnectionBean connectionBean) throws Exception {
		this.connectionBean = connectionBean;
		try {
			this.metaStoreBridge = MetaStoreBridgeFactory.getMetaStoreBridger(connectionBean);
		} catch(Exception e){
			LOG.error("Create Hive Meta Store Client failed: "+e.getMessage());
			throw e;
		}
	}

	/**
	 * create meta table
	 * 
	 * @param hiveTableName
	 * @param hbaseTableName
	 * @param hiveColumnNameList
	 * @param hbaseColumnNameList
	 * @param dataTypeList
	 * @return
	 */
	public MessageInfo createTable(String hiveTableName, String hbaseTableName, 
			List<String> hiveColumnNameList, List<String> hbaseColumnNameList, List<String> dataTypeList) {
		MessageInfo messageInfo = new MessageInfo();
		try{
			startTime = System.currentTimeMillis();
			HiveMetaStoreService metaStoreService = new HiveMetaStoreService(this.connectionBean);
			metaStoreService.createTable(hiveTableName, hbaseTableName, hiveColumnNameList, hbaseColumnNameList, dataTypeList);
			stopTime = System.currentTimeMillis();
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
	 * drop table
	 * 
	 * @param tableName meta table
	 * @return
	 */
	public MessageInfo dropTable(String tableName) {
		MessageInfo messageInfo = new MessageInfo();
		try{
			startTime = System.currentTimeMillis();
			HiveMetaStoreService metaStoreService = new HiveMetaStoreService(this.connectionBean);
			metaStoreService.dropTable(tableName);
			stopTime = System.currentTimeMillis();
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
	 * alter meta table
	 * 
	 * @param tableName :meta table name
	 * @param hiveColumnNameList
	 * @param hbaseColumnNameList
	 * @param dataTypeList
	 * @return
	 * @throws Exception
	 */
	public MessageInfo alterTable(String tableName, List<String> hiveColumnNameList, List<String> hbaseColumnNameList, List<String> dataTypeList) throws Exception{
		MessageInfo messageInfo = new MessageInfo();
		try {
			startTime = System.currentTimeMillis();
			TableBean tableBean = this.metaStoreBridge.describeTable(tableName);
			/*for sentry:測試是否有刪除的權限。*/
			if(this.metaStoreBridge.isHiveServer2Instance()){
				String tmpTableName = "HareTmp"+new Date().getTime();
				try{
					this.metaStoreBridge.createTable(tmpTableName, tableBean.getHbaseTableName(), 
							hiveColumnNameList, hbaseColumnNameList, dataTypeList);
					this.metaStoreBridge.dropTable(tmpTableName);
				}catch(Exception e){
					throw e;
				}
			}
			this.metaStoreBridge.dropTable(tableName);
			this.createTable(tableName, tableBean.getHbaseTableName(), hiveColumnNameList, hbaseColumnNameList, dataTypeList);
			stopTime = System.currentTimeMillis();
			messageInfo.setStatus(MessageInfo.SUCCESS);
			messageInfo.setResponseTime(stopTime - startTime);
		} catch (Exception e) {
			messageInfo.setStatus(MessageInfo.ERROR);
			messageInfo.setException("Alter Table["+tableName+"] failed: " + e.getMessage());
			return messageInfo;
		} finally{
			this.metaStoreBridge.close();
		}
		return messageInfo;
	}
	
	/**
	 * load Meta table info
	 * 
	 * @param hTableName
	 * @return
	 * @throws Exception
	 */
	public MetaTableBean describeTable(String hTableName) throws Exception {
		MetaTableBean metaBean = new MetaTableBean();
		try{
			startTime = System.currentTimeMillis();
			BeanUtils.copyProperties(metaBean, this.metaStoreBridge.describeTable(hTableName));
			stopTime = System.currentTimeMillis();
			metaBean.setStatus(MessageInfo.SUCCESS);
			metaBean.setResponseTime(stopTime - startTime);
		} catch (Exception e) {
			metaBean.setStatus(MessageInfo.ERROR);
			metaBean.setException("Describe Table["+hTableName+"] failed: " + e.getMessage());
			return metaBean;
		} finally{
			this.metaStoreBridge.close();
		}
		return metaBean;
	}
	
	/**
	 * load all meta table
	 * 
	 * @return
	 * @throws Exception
	 */
	public List<String> getAllTableNames() throws Exception{
		try{
			return this.metaStoreBridge.listHiveTableNames();
		} catch (Exception e) {
			LOG.error("Find all Hive Tables failed: " + e.getMessage());
			throw e;
		} finally{
			this.metaStoreBridge.close();
		}
	}
}