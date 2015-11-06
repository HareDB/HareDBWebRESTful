package com.haredb.harespark.operator;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

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
import com.haredb.harespark.bean.response.DescribeTableResponseBean;
import com.haredb.harespark.bean.response.PreviewResponseBean;
import com.haredb.harespark.bean.response.QueryStatusResponseBean;
import com.haredb.harespark.bean.response.QuerySubmitResponseBean;
import com.haredb.harespark.bean.response.ResponseInfoBean;
import com.haredb.harespark.bean.response.UploadDataFileResponseBean;
import com.haredb.harespark.bean.response.UploadDataFileStatusResponseBean;
import com.haredb.harespark.util.HareSparkFacade;

public class HareSparkOperatorIT {

	@Test
	public void testTableOperator() {
		HareSparkOperatorTestValues values = new HareSparkOperatorTestValues();
		HareSparkOperator operator = new HareSparkOperator(values.getUserSessionBean());
			
		try {
			CreateTableBean createTableBean = new CreateTableBean(values.getTableName(), values.getOriginCols(), values.getOriginDataType());
			ResponseInfoBean bean = operator.createTable(createTableBean);		
			assertEquals(bean.SUCCESS, bean.getStatus());
			
			DescribeTableBean describeTableBean = new DescribeTableBean(values.getTableName());
			DescribeTableResponseBean describeTableResponseBean = operator.describeTable(describeTableBean);
			
			assertEquals(values.getOriginCols(), describeTableResponseBean.getColumnNames());
			assertEquals(values.getOriginDataType(), describeTableResponseBean.getDataTypes());
			assertEquals(describeTableResponseBean.SUCCESS, describeTableResponseBean.getStatus());
		
			AlterTableBean alterTableBean = new AlterTableBean(values.getTableName(), values.getAlterCols(), values.getAlterDataType());
			bean = operator.alterTable(alterTableBean);
			assertEquals(bean.SUCCESS, bean.getStatus());
			
			describeTableResponseBean = operator.describeTable(describeTableBean);
			assertEquals(values.getAlterCols(), describeTableResponseBean.getColumnNames());
			assertEquals(values.getAlterDataType(), describeTableResponseBean.getDataTypes());
			assertEquals(describeTableResponseBean.SUCCESS, describeTableResponseBean.getStatus());
		}catch (Exception ex) {
			ex.printStackTrace();
		}finally {
			DropTableBean dropTableBean = new DropTableBean(values.getTableName());
			ResponseInfoBean bean = operator.dropTable(dropTableBean);
			assertEquals(bean.SUCCESS, bean.getStatus());
		}
		
		
		
			
	}

	
	@Test
	public void testFileOperator() {
		
		HareSparkOperatorTestValues values = new HareSparkOperatorTestValues();
		HareSparkOperator operator = new HareSparkOperator(values.getUserSessionBean());		
		
		try {
			CreateTableBean createTableBean = new CreateTableBean(values.getTableName(), values.getOriginCols(), values.getOriginDataType());
			ResponseInfoBean bean = operator.createTable(createTableBean);		
			assertEquals(bean.SUCCESS, bean.getStatus());
			
			values.uploadSampleDataFile();			

			UploadDataFileBean uploadDataFileBean = new UploadDataFileBean(values.getTableName(), values.getDataHadoopFile1(), values.getResultFolderPath1(), "", ",", UploadDataFileBean.operatorName_Normal);
			UploadDataFileResponseBean uploadDataFileResponseBean = operator.uploadDataFile(uploadDataFileBean);
			assertEquals(bean.SUCCESS, uploadDataFileResponseBean.getStatus());
			
			boolean flag = false;
			while (!flag) {
				UploadDataFileStatusBean uploadDataFileStatusBean = new UploadDataFileStatusBean("", "", values.getResultFolderPath1());
				UploadDataFileStatusResponseBean uploadDataFileStatusResponseBean = operator.uploadDataFileStatus(uploadDataFileStatusBean);
				System.out.println(uploadDataFileStatusResponseBean.toString());
				if (uploadDataFileStatusResponseBean.getStatus().equals(uploadDataFileStatusResponseBean.SUCCESS)) {
					flag = true;
				}
				Thread.sleep(1000);
			}
			
			PreviewBean previewBean = new PreviewBean(values.getTableName(), "1", "10");
			PreviewResponseBean previewResponseBean = operator.preview(previewBean);
			
			
			System.out.println(previewResponseBean.getHeads());
			print2DimensionalArray(previewResponseBean.getResults());
			
			uploadDataFileBean = new UploadDataFileBean(values.getTableName(), values.getDataHadoopFile2(), values.getResultFolderPath2(), "", ",", UploadDataFileBean.operatorName_Normal);
			uploadDataFileResponseBean = operator.uploadDataFile(uploadDataFileBean);
			assertEquals(bean.SUCCESS, uploadDataFileResponseBean.getStatus());
			
			flag = false;
			while (!flag) {
				UploadDataFileStatusBean uploadDataFileStatusBean = new UploadDataFileStatusBean("", "", values.getResultFolderPath1());
				UploadDataFileStatusResponseBean uploadDataFileStatusResponseBean = operator.uploadDataFileStatus(uploadDataFileStatusBean);
				System.out.println(uploadDataFileStatusResponseBean.toString());
				if (uploadDataFileStatusResponseBean.getStatus().equals(uploadDataFileStatusResponseBean.SUCCESS)) {
					flag = true;
				}
				Thread.sleep(1000);
			}
			
			previewBean = new PreviewBean(values.getTableName(), "1", "10");
			previewResponseBean = operator.preview(previewBean);
			System.out.println(previewResponseBean.getHeads());
			print2DimensionalArray(previewResponseBean.getResults());
			
			DeleteDataFileBean deleteDataFileBean = new DeleteDataFileBean(values.getTableName(), values.getDataFile1());
			bean = operator.deleteDataFile(deleteDataFileBean);
			assertEquals(bean.SUCCESS, bean.getStatus());
			
			previewBean = new PreviewBean(values.getTableName(), "1", "10");
			previewResponseBean = operator.preview(previewBean);
			System.out.println(previewResponseBean.getHeads());
			print2DimensionalArray(previewResponseBean.getResults());
		}catch(Exception ex) {
			ex.printStackTrace();
		}finally{
			values.deleteSampleDataFile();
			values.deleteSampleResultFolder();
			
			DropTableBean dropTableBean = new DropTableBean(values.getTableName());
			ResponseInfoBean bean = operator.dropTable(dropTableBean);
			assertEquals(bean.SUCCESS, bean.getStatus());
		}
		
		
	}
	
	
	@Test
	public void testSQLOperator() {	
		
		HareSparkOperatorTestValues values = new HareSparkOperatorTestValues();
		HareSparkOperator operator = new HareSparkOperator(values.getUserSessionBean());		
		
		try {
			/*
			 * *******  prepare data start
			 */
			
			CreateTableBean createTableBean = new CreateTableBean(values.getTableName(), values.getOriginCols(), values.getOriginDataType());
			ResponseInfoBean bean = operator.createTable(createTableBean);		
			assertEquals(bean.SUCCESS, bean.getStatus());
			
				values.uploadSampleDataFile();
			
			UploadDataFileBean uploadDataFileBean = new UploadDataFileBean(values.getTableName(), values.getDataHadoopFile1(), values.getResultFolderPath1(), "", ",", UploadDataFileBean.operatorName_Normal);
			UploadDataFileResponseBean uploadDataFileResponseBean = operator.uploadDataFile(uploadDataFileBean);
			assertEquals(bean.SUCCESS, uploadDataFileResponseBean.getStatus());
			
			boolean flag = false;
			while (!flag) {
				UploadDataFileStatusBean uploadDataFileStatusBean = new UploadDataFileStatusBean("", "", values.getResultFolderPath1());
				UploadDataFileStatusResponseBean uploadDataFileStatusResponseBean = operator.uploadDataFileStatus(uploadDataFileStatusBean);
				System.out.println(uploadDataFileStatusResponseBean.toString());
				if (uploadDataFileStatusResponseBean.getStatus().equals(uploadDataFileStatusResponseBean.SUCCESS)) {
					flag = true;
				}
				Thread.sleep(1000);
			}
			
			PreviewBean previewBean = new PreviewBean(values.getTableName(), "1", "10");
			PreviewResponseBean previewResponseBean = operator.preview(previewBean);		
			System.out.println(previewResponseBean.getHeads());
			print2DimensionalArray(previewResponseBean.getResults());
			
			values.deleteSampleResultFolder();
			/*
			 * *******  prepare data end
			 */
			
			QuerySubmitBean querySubmitBean = new QuerySubmitBean(values.getSQL(), values.getTableName(), values.getResultFolderPath1());
			QuerySubmitResponseBean querySubmitResponseBean = operator.querySubmit(querySubmitBean);
			assertEquals(bean.SUCCESS, querySubmitResponseBean.getStatus());
			
			flag = false;
			while (!flag) {				
				QueryStatusBean queryStatusBean = new QueryStatusBean(querySubmitResponseBean.getQueryJobName());
				QueryStatusResponseBean queryStatusResponseBean = operator.queryStatus(queryStatusBean);
				System.out.println(queryStatusResponseBean.toString());
				if (queryStatusResponseBean.getJobStatus().equals(queryStatusResponseBean.FINISHED)) {
					flag = true;
				}
				Thread.sleep(1000);
			}
			values.viewSQLResultData(values.getResultFolderPath1());
			
		}catch (Exception ex) {
			ex.printStackTrace();			
		}finally {
			values.deleteSampleDataFile();
			values.deleteSampleResultFolder();
			
			DropTableBean dropTableBean = new DropTableBean(values.getTableName());
			ResponseInfoBean bean = operator.dropTable(dropTableBean);
			assertEquals(bean.SUCCESS, bean.getStatus());
		}		
		
	}
	
	
	
	@Test
	public void sss() {
		HareSparkOperatorTestValues values = new HareSparkOperatorTestValues();
		HareSparkOperator operator = new HareSparkOperator(values.getUserSessionBean());		
		try {
			values.deleteSampleDataFile();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			values.deleteSampleResultFolder();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		UploadDataFileBean uploadDataFileBean = new UploadDataFileBean(values.getTableName(), values.getDataFile1(), values.getResultFolderPath(), "", ",", UploadDataFileBean.operatorName_Normal);
//		UploadDataFileResponseBean uploadDataFileResponseBean = operator.uploadDataFile(uploadDataFileBean);
//		PreviewBean previewBean = new PreviewBean(values.getTableName(), "0", "10");
//		PreviewResponseBean previewResponseBean = operator.preview(previewBean);
//		DropTableBean dropTableBean = new DropTableBean(values.getTableName());
//		operator.dropTable(dropTableBean);
	}
	
	
	private void print2DimensionalArray(String[][] data) {
		for(int i = 0; i < data.length; i++) {
			String temp = "";
			for(int j = 0; j < data[i].length; j++) {
				temp = temp + "," + data[i][j];
			}
			System.out.println(temp);		
		}	
				
	}
	
	
	
	
}
