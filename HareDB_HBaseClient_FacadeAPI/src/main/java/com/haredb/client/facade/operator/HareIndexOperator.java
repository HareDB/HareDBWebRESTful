package com.haredb.client.facade.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.haredb.adapter.bean.ErrorBean;
import com.haredb.adapter.bean.CommonBean.EnumErrorType;
import com.haredb.adapter.index.IndexOperator;
import com.haredb.client.facade.bean.IndexBean;
import com.haredb.client.facade.bean.IndexStatusBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.until.HareEnv;
import com.haredb.hbaseclient.core.Connection;

public class HareIndexOperator extends HareContrivance{
//	private static Logger logger = Logger.getLogger(HareIndexOperator.class);
	
	private Properties indexProp = new Properties();
	final IndexBean indexBean;
	private String containerRealPath;
	private String jobName = null;
	
	private static final int TYPE_CREATE=1,TYPE_UPDATE=2; 
	
	
	public HareIndexOperator(Connection connection, IndexBean indexBean){
		super(connection);
		this.indexBean=indexBean;
	}
	
	
	public IndexStatusBean createIndex() throws Exception{
		IndexStatusBean statusBean = indexFunction(HareIndexOperator.TYPE_CREATE);
		return statusBean;
	}
	public IndexStatusBean updateIndex(){
		IndexStatusBean statusBean;
		statusBean = indexFunction(HareIndexOperator.TYPE_UPDATE);
		return statusBean;
	}
	
	
	private IndexStatusBean indexFunction(int indexType) {
		final int type = indexType; 
		IndexStatusBean statusBean = new IndexStatusBean();
		long startTime = System.currentTimeMillis();
		try {
			this.loadSchemaProperties(connection);
		} catch (Exception e) {
			statusBean.setStatus(MessageInfo.ERROR);
			statusBean.setException(printStackTrace(e));
			return statusBean;
		}
		Thread indexThread = new Thread(new Runnable() {
			@Override
			public void run() {
				String zkTmpPath = getZkTmpPath();
				String jarPath = getIndexJarPath();
				
				try {
					String tableName 					= getTableName();
					String collectionName			= tableName;
					List<String> collectionList 	= getCollectionList();
					System.out.println("---------------jarpath:" + jarPath);
					System.out.println("---------------zkTmpPath:" + zkTmpPath);
					IndexOperator operator = new IndexOperator(connection,zkTmpPath,jarPath);
					ErrorBean errorBean;
					switch (type) {
					case HareIndexOperator.TYPE_CREATE:
						errorBean = operator.startCreateIndexJob(collectionName, collectionList);						
						break;

					case HareIndexOperator.TYPE_UPDATE:
						errorBean = operator.startUpdateIndexJob(collectionName, collectionList, new ArrayList<String>());						
						break;
					default:
						errorBean = new ErrorBean();
						errorBean.setErrorType(EnumErrorType.ERROR);
						errorBean.setErrorMsg("No Consistent type of index job");
						break;
					}				
					if(errorBean.getErrorType().equals(EnumErrorType.ERROR)){
						throw new Exception(errorBean.getErrorObj()); 
					}
				}catch(Exception e) {
					/* for error  */
					MessageInfo info = new MessageInfo();
					info.setStatus(MessageInfo.ERROR);
					info.setException(e.getMessage());
					try {
						writeFileToHdfs(info, indexBean.getResultPath(),true);
					} catch (Exception e1) {
						throw new RuntimeException(e1);
					}
				}
			}
			
		});
		indexThread.start();
		
		statusBean.setStatus(MessageInfo.SUCCESS);
		long endTime = System.currentTimeMillis();
		statusBean.setResponseTime(endTime - startTime);
		return statusBean;
	}
	
	
	/**
	 * drop index by table name
	 * */
	public IndexStatusBean dropIndex(){
		IndexStatusBean statusBean = new IndexStatusBean();
		statusBean.setStatus(MessageInfo.SUCCESS);
		String zkTmpPath = getZkTmpPath();
		String jarPath = getIndexJarPath();
		String tableName 					= getTableName();
		String collectionName			= tableName;
		try {
			IndexOperator operator = new IndexOperator(connection,zkTmpPath,jarPath);
			operator.dropSchemaByProxy(collectionName);
		} catch (Exception e) {
			statusBean.setStatus(MessageInfo.ERROR);
			statusBean.setException(e.getMessage());
			try {
				writeFileToHdfs(statusBean, indexBean.getResultPath(),true);
			} catch (Exception e1) {
				throw new RuntimeException(e1);
			}
		}
		return statusBean;
	}
	
	
	
	protected String getZkTmpPath() {
		return System.getProperty("user.home")+this.indexProp.getProperty("zktmppath");
	}

	protected String getIndexJarPath() {
		return HareEnv.getIndexJar().getPath();
	}

	protected List<String> getCollectionList() {
		StringUtils until = new StringUtils();
		return Arrays.asList(until.split(indexBean.getCollectionList(),","));
	}

	protected String getTableName() {
		return this.indexBean.getTableName();
	}

	protected void loadSchemaProperties(Connection connection) throws Exception{
		this.indexProp.load(HareIndexOperator.class.getResourceAsStream("/index.prop"));
		
	}
	

}
