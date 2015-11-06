package com.haredb.harespark.operator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.haredb.harespark.bean.TableColumn;
import com.haredb.harespark.bean.TableInfo;
import com.haredb.harespark.bean.input.AlterTableBean;
import com.haredb.harespark.bean.input.CreateTableBean;
import com.haredb.harespark.bean.input.DeleteDataFileBean;
import com.haredb.harespark.bean.input.DescribeTableBean;
import com.haredb.harespark.bean.input.DropTableBean;
import com.haredb.harespark.bean.input.PreviewBean;
import com.haredb.harespark.bean.input.QueryStatusBean;
import com.haredb.harespark.bean.input.QuerySubmitBean;
import com.haredb.harespark.bean.input.UploadDataFileBean;
import com.haredb.harespark.bean.input.UploadDataFileStatusBean;
import com.haredb.harespark.bean.input.UserSessionBean;
import com.haredb.harespark.bean.response.DescribeTableResponseBean;
import com.haredb.harespark.bean.response.PreviewResponseBean;
import com.haredb.harespark.bean.response.QueryStatusResponseBean;
import com.haredb.harespark.bean.response.QuerySubmitResponseBean;
import com.haredb.harespark.bean.response.ResponseInfoBean;
import com.haredb.harespark.bean.response.UploadDataFileResponseBean;
import com.haredb.harespark.bean.response.UploadDataFileStatusResponseBean;
import com.haredb.harespark.util.HareSparkFacade;

public class HareSparkOperator {
	
	private long startTime = 0;
	private long stopTime = 0;

	private UserSessionBean userSessionBean;
	private HareSparkFacade hareSparkFacade;
	
	public HareSparkOperator() {
		
	}
	
	public HareSparkOperator(UserSessionBean userSessionBean) {
		this.userSessionBean = userSessionBean;
	}
	
	
	public ResponseInfoBean createUserSession(UserSessionBean userSessionBean) {
		ResponseInfoBean bean = new ResponseInfoBean();
		
		startTime = System.currentTimeMillis();
		String folder = userSessionBean.getConfigurationFolderPath();
		File file = new File(folder);
		if (file.exists()) {
			bean.setStatus(bean.SUCCESS);
		}else {
			bean.setStatus(bean.ERROR);
			bean.setException("Folder path '" + folder +"' is not exist. " );
		}
		stopTime = System.currentTimeMillis();
		bean.setResponseTime(stopTime - startTime);
		return bean;
	}
	
	
	public ResponseInfoBean createTable(CreateTableBean createTableBean) {
		ResponseInfoBean bean = new ResponseInfoBean();
		
		startTime = System.currentTimeMillis();
		try {
			hareSparkFacade = new HareSparkFacade(userSessionBean.getConfigurationFolderPath());
			hareSparkFacade.createTable(createTableBean.getTableName(), createTableBean.getColumnNames(), createTableBean.getDataType());
			bean.setStatus(bean.SUCCESS);
		}catch(Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException("Createtable failed : \n" + e.getMessage());
			e.printStackTrace();
		}		
		stopTime = System.currentTimeMillis();
		bean.setResponseTime(stopTime - startTime);	
		return bean;
	}
	
	public UploadDataFileResponseBean uploadDataFile(UploadDataFileBean uploadDataFileBean) {
		UploadDataFileResponseBean bean = new UploadDataFileResponseBean();
		startTime = System.currentTimeMillis();		
		// Todo : change to thread	
		try {			
			hareSparkFacade = new HareSparkFacade(userSessionBean.getConfigurationFolderPath());
			boolean skipHeader = false;
			try {
				skipHeader = Boolean.valueOf(uploadDataFileBean.getSkipHeader());
			}catch (Exception e) {
				throw new Exception("skipHeader value " + uploadDataFileBean.getSkipHeader() + " is error");
			}			
			asychronizeRunnable runnable = new asychronizeRunnable(hareSparkFacade, uploadDataFileBean);
			new Thread(runnable).start();			
//			hareSparkFacade.uploadDataFile(uploadDataFileBean.getDataFilePath(), uploadDataFileBean.getResultPath(), uploadDataFileBean.getCsvSeparator(), uploadDataFileBean.getTableName(), skipHeader, uploadDataFileBean.getOperator());
			
			bean.setUploadJobName(uploadDataFileBean.getResultPath());
			bean.setStatus(bean.SUCCESS);
		}catch(Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException("uploadDataFile failed : \n" + e.getMessage());
			e.printStackTrace();
		}	
		stopTime = System.currentTimeMillis();
		bean.setResponseTime(stopTime - startTime);	
		return bean;
		
	}	
	
	public UploadDataFileStatusResponseBean uploadDataFileStatus(UploadDataFileStatusBean uploadDataFileStatusBean) {
		UploadDataFileStatusResponseBean bean = new UploadDataFileStatusResponseBean();
		startTime = System.currentTimeMillis();		
		try {
			hareSparkFacade = new HareSparkFacade(userSessionBean.getConfigurationFolderPath());
			String response = hareSparkFacade.uploadFileStatus(uploadDataFileStatusBean.getUploadJobName());
			
			String[] responseinfo = response.split(",");
			bean.setStartTime(responseinfo[0]);
			bean.setFinishTime(responseinfo[1]);
			bean.setDataStatus(responseinfo[2]);
			bean.setStatus(bean.SUCCESS);
		}catch(Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException("uploadDataFileStatus failed : \n" + e.getMessage());
			e.printStackTrace();
		}	
		stopTime = System.currentTimeMillis();
		bean.setResponseTime(stopTime - startTime);	
		return bean;
		
	}
	
	public QuerySubmitResponseBean querySubmit(QuerySubmitBean querySubmitBean) {
		QuerySubmitResponseBean bean = new QuerySubmitResponseBean();
		startTime = System.currentTimeMillis();		
		try {
			String jobname = querySubmitBean.getTablename() + "_" + System.currentTimeMillis();
			
			
			hareSparkFacade = new HareSparkFacade(userSessionBean.getConfigurationFolderPath());
			asychronizeRunnable runnable = new asychronizeRunnable(hareSparkFacade, querySubmitBean, jobname);
			new Thread(runnable).start();
//			hareSparkFacade.queryHDFSTable(querySubmitBean.getResultFileFolder(), querySubmitBean.getSql(), querySubmitBean.getTablename(), jobname);
			bean.setQueryJobName(jobname);
			bean.setStatus(bean.SUCCESS);
		}catch(Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException("querySubmit failed : \n" + e.getMessage());
			e.printStackTrace();
		}	
		stopTime = System.currentTimeMillis();
		bean.setResponseTime(stopTime - startTime);	
		return bean;
		
	}
	
	
	public QueryStatusResponseBean queryStatus(QueryStatusBean queryStatusBean) {
		QueryStatusResponseBean bean = new QueryStatusResponseBean();
		startTime = System.currentTimeMillis();		
		try {
			hareSparkFacade = new HareSparkFacade(userSessionBean.getConfigurationFolderPath());			
			List<String> responselist = hareSparkFacade.queryHDFSTableStatus(queryStatusBean.getQueryJobName());
			bean.setJobStatus(responselist.get(0));
			bean.setJobID(responselist.get(1));
			bean.setJobName(responselist.get(2));
			bean.setJobFinishTime(responselist.get(3));
			bean.setJobStartTime(responselist.get(4));
			bean.setJobProgress(responselist.get(5));
			
			bean.setStatus(bean.SUCCESS);
		}catch(Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException("queryStatus failed : \n" + e.getMessage());
			e.printStackTrace();
		}
		stopTime = System.currentTimeMillis();
		bean.setResponseTime(stopTime - startTime);	
		return bean;
		
	}
	
	
	public PreviewResponseBean preview(PreviewBean previewBean) {
		PreviewResponseBean bean = new PreviewResponseBean();
		startTime = System.currentTimeMillis();		
		try {
			hareSparkFacade = new HareSparkFacade(userSessionBean.getConfigurationFolderPath());
			int pagesize = 0;
			int limit = 0;
			
			try {
				pagesize = Integer.valueOf(previewBean.getPageSize());
				limit = Integer.valueOf(previewBean.getLimit());
			}catch (Exception e) {
				throw new Exception(e.getMessage());
			}			
			List<String> responselist = hareSparkFacade.previewData(previewBean.getTableName(), pagesize, limit);
			for(String dd :responselist) {
				System.out.println("hareSparkFacade : " + dd);
			}			
			
			if (responselist == null || responselist.size() == 0) {
				throw new Exception("table " + previewBean.getTableName() + " is not created");
			}
			
			
			String[] header = responselist.get(0).split(",");
			
			String[][] results = new String[responselist.size() - 1][];
			if (responselist.size() != 1) {
				for(int i = 1; i < responselist.size(); i++) {
					String[] temp = responselist.get(i).split(",");
					results[i - 1] = new String[temp.length];
					for (int j = 0; j < temp.length; j++) {
						results[i - 1][j] = temp[j];
					}					
				}
			}
			
			
			bean.setHeads(header);
			bean.setResults(results);
			bean.setStatus(bean.SUCCESS);
		}catch(Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException("querySubmit failed : \n" + e.getMessage());
			e.printStackTrace();
		}
		stopTime = System.currentTimeMillis();
		bean.setResponseTime(stopTime - startTime);	
		return bean;
		
	}
	
	public ResponseInfoBean deleteDataFile(DeleteDataFileBean deleteDataFileBean) {
		ResponseInfoBean bean = new ResponseInfoBean();
		startTime = System.currentTimeMillis();		

		try {
			hareSparkFacade = new HareSparkFacade(userSessionBean.getConfigurationFolderPath());
			boolean result = hareSparkFacade.deleteDataFile(deleteDataFileBean.getTableName(), deleteDataFileBean.getDeleteDataFileName());			
			bean.setStatus(bean.SUCCESS);			
		}catch(Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException("deleteDataFilefailed : \n" + e.getMessage());
			e.printStackTrace();
		}	
		stopTime = System.currentTimeMillis();
		bean.setResponseTime(stopTime - startTime);	
		return bean;
		
	}
	
	public ResponseInfoBean dropTable(DropTableBean dropTableBean) {
		ResponseInfoBean bean = new ResponseInfoBean();
		startTime = System.currentTimeMillis();		

		try {
			hareSparkFacade = new HareSparkFacade(userSessionBean.getConfigurationFolderPath());
			hareSparkFacade.dropTable(dropTableBean.getTableName());
			bean.setStatus(bean.SUCCESS);
		}catch(Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException("Drop table failed : \n" + e.getMessage());
			e.printStackTrace();
		}	
		stopTime = System.currentTimeMillis();
		bean.setResponseTime(stopTime - startTime);	
		return bean;
		
	}
	
	public ResponseInfoBean alterTable(AlterTableBean alterTableBean) {
		ResponseInfoBean bean = new ResponseInfoBean();
		startTime = System.currentTimeMillis();		
		try {
			hareSparkFacade = new HareSparkFacade(userSessionBean.getConfigurationFolderPath());
			hareSparkFacade.alterTable(alterTableBean.getTableName(), alterTableBean.getColumnNames(), alterTableBean.getDataTypes());
			bean.setStatus(bean.SUCCESS);
		}catch(Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException("Alter table failed : \n" + e.getMessage());
			e.printStackTrace();
		}	
		stopTime = System.currentTimeMillis();
		bean.setResponseTime(stopTime - startTime);	
		return bean;
		
	}
	
	public DescribeTableResponseBean describeTable(DescribeTableBean describeTableBean) {
		DescribeTableResponseBean bean = new DescribeTableResponseBean();
		startTime = System.currentTimeMillis();		
		try {
			hareSparkFacade = new HareSparkFacade(userSessionBean.getConfigurationFolderPath());
			TableInfo tableinfo = hareSparkFacade.describeTable(describeTableBean.getTableName());			
			
			if (tableinfo == null) {
				throw new Exception("Table " + describeTableBean.getTableName() + " is not existed");
			}
			List<String> listColumns = new ArrayList<String>();
			List<String> listDataType = new ArrayList<String>();
			List<TableColumn> list = tableinfo.getColumns();
			for (TableColumn col : list) {
				listColumns.add(col.getColumnName());
				listDataType.add(col.getDataType());
			}
			
			bean.setColumnNames(listColumns);
			bean.setDataTypes(listDataType);
			bean.setStatus(bean.SUCCESS);
		}catch(Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException("Describe table failed : \n" + e.getMessage());
			e.printStackTrace();
		}	
		stopTime = System.currentTimeMillis();
		bean.setResponseTime(stopTime - startTime);	
		return bean;
		
	}
	
	public boolean isExists(String tableName){
		hareSparkFacade = new HareSparkFacade(userSessionBean.getConfigurationFolderPath());
		return hareSparkFacade.isExists(tableName);
	}
	
	private class asychronizeRunnable implements Runnable {

//		Runnable runnable = new asychronizeRunnable(hareSparkFacade, uploadDataFileBean); 
//		new Thread(runnable).start();
		private HareSparkFacade hareSparkFacade;
		private UploadDataFileBean uploadDataFileBean;
		private QuerySubmitBean querySubmitBean;
		private String jobname = "";
		private int actionType = 0;
		
		public asychronizeRunnable(HareSparkFacade hareSparkFacade, UploadDataFileBean uploadDataFileBean) {
			this.hareSparkFacade = hareSparkFacade;
			this.uploadDataFileBean = uploadDataFileBean;
			this.actionType = 1;
		}
		
		public asychronizeRunnable(HareSparkFacade hareSparkFacade, QuerySubmitBean querySubmitBean, String jobname) {
			this.hareSparkFacade = hareSparkFacade;
			this.querySubmitBean = querySubmitBean;
			this.jobname = jobname;
			this.actionType = 2;
		}
		
		@Override
		public void run() {
			if (this.actionType == 1) {
				hareSparkFacade.uploadDataFile(uploadDataFileBean.getDataFilePath(), uploadDataFileBean.getResultPath(), uploadDataFileBean.getCsvSeparator(), uploadDataFileBean.getTableName(), Boolean.valueOf(uploadDataFileBean.getSkipHeader()), uploadDataFileBean.getOperator());			
			}else if (this.actionType == 2) {
				hareSparkFacade.queryHDFSTable(querySubmitBean.getResultFileFolder(), querySubmitBean.getSql(), querySubmitBean.getTablename(), jobname);
			}
			
		}
		
	}
	
		
}
