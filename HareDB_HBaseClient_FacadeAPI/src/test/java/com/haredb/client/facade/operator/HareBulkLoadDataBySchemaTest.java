package com.haredb.client.facade.operator;

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

import com.haredb.client.facade.bean.BulkFileBean;
import com.haredb.client.facade.bean.BulkTableBean;
import com.haredb.client.facade.bean.BulkloadBean;
import com.haredb.client.facade.bean.UploadSchemaBean;
import com.haredb.hbaseclient.core.Connection;

public class HareBulkLoadDataBySchemaTest {
	@Test
	public void testrunSchemaBulkload1(){
		try {
			Connection connection = new Connection("host1", "2181");
			connection.setNameNodeHostPort("hdfs://host1:9000");
			UploadSchemaBean uploadSchemaBean = new UploadSchemaBean();
			uploadSchemaBean.setSchemaFilePath("hdfs://host1:9000/schema.prop");
			HareBulkLoadDataBySchema bulkloadBySchema = new HareBulkLoadDataBySchema(connection,uploadSchemaBean);
			bulkloadBySchema.loadSchemaProperties(connection);
			BulkloadBean bulkloadBean = bulkloadBySchema.getBulkloadBean();
			BulkTableBean bulkloadTableBean = bulkloadBySchema.getBulkTableBean();
			BulkFileBean bulkFileBean = bulkloadBySchema.getBulkFileBean();
			
			assertEquals("/home/user1/bulkload.jar", bulkloadBean.getJarPath());
			assertEquals("HFile", bulkloadBean.getBulkloadType());
			assertEquals("false", bulkloadTableBean.getCompression());
			assertEquals("", bulkloadTableBean.getStartKey());
			assertEquals("", bulkloadTableBean.getEndKey());
			assertEquals("table100", bulkloadTableBean.getTableName());
			assertEquals("HDFS", bulkFileBean.getFileLocation());
			assertEquals("NORMAL", bulkFileBean.getFileType());
			assertEquals("hdfs://host1:9000/result", bulkFileBean.getOutputFolder());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void testrunSchemaBulkload2(){
		Connection connection = new Connection("host1", "2181");
		connection.setNameNodeHostPort("hdfs://host1:9000");
		connection.setRmAddressHostPort("host1:8032");
		connection.setRmSchedulerAddressHostPort("host1:8030");
		connection.setRmResourceTrackerAddressHostPort("host1:8031");
		connection.setRmAdminAddressHostPort("host1:8033");
		connection.setYarnNodeManagerAuxServices("mapreduce_shuffle");
		connection.setMrJobhistoryAddress("host1:10020");
		
		UploadSchemaBean schemaBean = new UploadSchemaBean();
		schemaBean.setSchemaFilePath("hdfs://host1:9000/schema.prop");
		schemaBean.setDataPath("hdfs://host1:9000/aaa.txt");
		
		HareBulkLoadDataBySchema bulkloadBySchema = new HareBulkLoadDataBySchema(connection, schemaBean);
		
		bulkloadBySchema.runSchemaBulkload();
	}
	
	@Test
	public void testHDFSFile(){
		try{
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "host1");
		    config.set("hbase.zookeeper.property.clientPort", "2181");
		    config.set("fs.default.name", "hdfs://host1:9000");
			
		    FileSystem fs = FileSystem.get(config);
		    FSDataInputStream inStream = fs.open(new Path("/schema.prop"));
		    Properties prop = new Properties();
		    prop.load(inStream);
		    System.out.println(prop.getProperty("bulkloadoutputfolder"));
		    System.out.println(prop.getProperty("bulkloadfileformat"));
		}catch(Exception e){
			throw new RuntimeException("Exception:", e);
		}
	}
}
