package com.haredb.client.rest.resource;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.gson.Gson;
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


public class HareSparkResourceTest{

	private static String clusterHost = "hdfs://host1:8020";
	private static HttpClient client = new HttpClient();
	private static String httpUrlRoot = "http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/harespark";
	private static String contentType = "application/json";
	private static String clusterConfiguration = "/home/user1/hadoop4";
	private static String charSet = "UTF-8";
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
		PostMethod method = new PostMethod(httpUrlRoot + "/usersession");
		String json = HareSparkResourceTest.objectToJsonStr(HareSparkResourceTest.getUserSession());
		
		StringRequestEntity requestEntity = new StringRequestEntity(json, contentType, charSet);
		method.setRequestEntity(requestEntity);
			
		int statusCode = client.executeMethod(method);
		assertEquals(200, statusCode);//test create session
	}
	
	
	@Test
	public void testCreateTable() throws Exception{
		String tableName = "testtable";
		if(this.isExists(tableName)){
			this.dropTable(tableName);
		}
		ResponseInfoBean response = this.createTable(tableName);
		assertEquals("success", response.getStatus());
	}
	
	@Test
	public void testAlterTable() throws Exception{
		testCreateTable();
		String tableName = "testtable";

		PostMethod method = new PostMethod(httpUrlRoot + "/altertable");
		AlterTableBean alterTableBean = new AlterTableBean();
		alterTableBean.setTableName(tableName);
		alterTableBean.setColumnNames(Arrays.asList("col1", "col2"));
		alterTableBean.setDataTypes(Arrays.asList("string", "string"));
		
		String json = HareSparkResourceTest.objectToJsonStr(alterTableBean);
		
		StringRequestEntity requestEntity = new StringRequestEntity(json, contentType, charSet);
		method.setRequestEntity(requestEntity);
		int status = client.executeMethod(method);
		assertEquals(200, status);
		
		byte[] resultByte = method.getResponseBody(1024 * 1024 * 10);
		String result = new String(resultByte);
		
		Gson gson = new Gson();
		ResponseInfoBean response = gson.fromJson(result, ResponseInfoBean.class);
		assertEquals("success", response.getStatus());
	}
	
	@Test
	public void testDropExistsTable() throws Exception{//table exists
		String tableName = "table5";
		if(!this.isExists(tableName)){
			ResponseInfoBean response = this.createTable(tableName);
			assertEquals("success", response.getStatus());
		}
		
		String result = this.dropTable(tableName);
		assertEquals("success", result);
	}
	@Test
	public void testDropNotExistsTable() throws Exception{//table not found
		String result = this.dropTable("tableName1111");
		assertEquals("error", result);
	}

	@Test
	public void testIsNotExists() throws Exception{
		String tableName = "xxxxxxx";
		PostMethod method = new PostMethod(httpUrlRoot + "/isExists");
		
		StringRequestEntity requestEntity = new StringRequestEntity(tableName, contentType, charSet);
		method.setRequestEntity(requestEntity);
		client.executeMethod(method);
		byte[] resultByte = method.getResponseBody(1024 * 1024 * 10);
		String result = new String(resultByte);
		
		Gson gson = new Gson();
		assertEquals("false", gson.fromJson(result, String.class));
	}
	
	@Test
	public void testDescribeTable() throws Exception{
		String tableName = "table1";
		DescribeTableResponseBean describeTable = this.tableDescribe(tableName);
		assertEquals(5, describeTable.getColumnNames().size());
		assertEquals(5, describeTable.getDataTypes().size());
	}
	
	@Test
	public void testUploadDataFile() throws Exception{
		String jobName = this.uploadFile("uploadTable");
		assertTrue(jobName != null);
	}
	
	@Test
	public void testUploadDataFileStatus() throws Exception{
		String jobName = this.uploadFile("uploadTable");
		
		PostMethod method = new PostMethod(httpUrlRoot + "/uploaddatafile/status");
		UploadDataFileStatusBean uploadDataFileStatusBean = new UploadDataFileStatusBean();
		uploadDataFileStatusBean.setUploadJobName(jobName);
		
		String json = HareSparkResourceTest.objectToJsonStr(uploadDataFileStatusBean);
		
		StringRequestEntity requestEntity = new StringRequestEntity(json, contentType, charSet);
		method.setRequestEntity(requestEntity);
		client.executeMethod(method);
		
		byte[] resultByte = method.getResponseBody(1024 * 1024 * 20);
		String result = new String(resultByte);
		
		Gson gson = new Gson();
		UploadDataFileStatusResponseBean response = gson.fromJson(result, UploadDataFileStatusResponseBean.class);
		assertEquals(response.getStatus(), "success");
		assertTrue(response.getException() == null);
	}
	
	@Test
	public void testQuerySubmit() throws Exception{
		String tableName = "queryTable";
		if(this.isExists(tableName)){
			this.dropTable(tableName);
		}
		this.createTable(tableName);
		this.uploadFile(tableName);
		
		Configuration config = new Configuration();
		config.addResource(new Path(clusterConfiguration + "/core-site.xml"));
		
		FileSystem fs = FileSystem.get(config);
		
		String tempFilePatnAndName = "/tmp/queryresult";
		Path path = new Path(tempFilePatnAndName);
		if(fs.exists(path)){
			fs.delete(path, true);
		}
	
		PostMethod method = new PostMethod(httpUrlRoot + "/querysubmit");
		QuerySubmitBean querySubmitBean = new QuerySubmitBean();
		querySubmitBean.setResultFileFolder(tempFilePatnAndName);
		querySubmitBean.setTablename(tableName);
		querySubmitBean.setSql("SELECT * FROM " + tableName);
		
		String json = HareSparkResourceTest.objectToJsonStr(querySubmitBean);
		
		StringRequestEntity requestEntity = new StringRequestEntity(json, contentType, charSet);
		method.setRequestEntity(requestEntity);
		client.executeMethod(method);
		
		byte[] resultByte = method.getResponseBody(1024 * 1024 * 10);
		String result = new String(resultByte);
		
		Gson gson = new Gson();
		QuerySubmitResponseBean response = gson.fromJson(result, QuerySubmitResponseBean.class);
		
		
		System.out.println(response.getAuth());
		System.out.println(response.getStatus());
	}
	@Ignore
	@Test
	public void testQueryStatus() throws Exception{
		PostMethod method = new PostMethod(httpUrlRoot + "/querystatus");
		QueryStatusBean queryStatusBean = new QueryStatusBean();
		//TODO set appliationName
		
		String json = HareSparkResourceTest.objectToJsonStr(queryStatusBean);
		
		StringRequestEntity requestEntity = new StringRequestEntity(json, contentType, charSet);
		method.setRequestEntity(requestEntity);
		client.executeMethod(method);
		
		byte[] resultByte = method.getResponseBody(1024 * 1024 * 10);
		String result = new String(resultByte);
		
		Gson gson = new Gson();
		QueryStatusResponseBean response = gson.fromJson(result, QueryStatusResponseBean.class);
		response.getJobStatus();
	}
	@Ignore
	@Test
	public void testPreview() throws Exception{//TODO String array
		String tableName = "previewtable";
		if(this.isExists(tableName)){
			this.dropTable(tableName);
		}
		this.createTable(tableName);
		this.uploadFile(tableName);
		
		PostMethod method = new PostMethod(httpUrlRoot + "/preview");
		PreviewBean previewBean = new PreviewBean();
		previewBean.setLimit("10");
		previewBean.setPageSize("1");
		previewBean.setTableName(tableName);
		
		String json = HareSparkResourceTest.objectToJsonStr(previewBean);
		StringRequestEntity requestEntity = new StringRequestEntity(json, contentType, charSet);
		method.setRequestEntity(requestEntity);
		client.executeMethod(method);
		
		byte[] resultByte = method.getResponseBody(1024 * 1024 * 10);
		String result = new String(resultByte);
		
		Gson gson = new Gson();
		PreviewResponseBean response = gson.fromJson(result, PreviewResponseBean.class);
		String[] results = response.getResults().split(",");
		for(String r : results){
			System.out.println(r);
		}
		
	}
	@Test
	public void testDeteDataFile() throws Exception{
		PostMethod method = new PostMethod(httpUrlRoot + "/deletedatafile");
		DeleteDataFileBean deleteDataFileBean = new DeleteDataFileBean();
		deleteDataFileBean.setTableName("table1");
		deleteDataFileBean.setDeleteDataFileName("origin.txt");
		
		String json = HareSparkResourceTest.objectToJsonStr(deleteDataFileBean);
		StringRequestEntity requestEntity = new StringRequestEntity(json, contentType, charSet);
		method.setRequestEntity(requestEntity);
		client.executeMethod(method);
		
		byte[] resultByte = method.getResponseBody(1024 * 1024 * 10);
		String result = new String(resultByte);
		
		Gson gson = new Gson();
		ResponseInfoBean response = gson.fromJson(result, ResponseInfoBean.class);
		System.out.println(response.getStatus());
	}

	private static String objectToJsonStr(Object object){
		Gson gson = new Gson();
		return gson.toJson(object);
	}
	
	private static UserSessionBean getUserSession(){
		UserSessionBean userSessionBean = new UserSessionBean();
		userSessionBean.setConfigurationFolderPath(clusterConfiguration);
		return userSessionBean;
	}
	
	private ResponseInfoBean createTable(String tableName) throws Exception{
		PostMethod method = new PostMethod(httpUrlRoot + "/createtable");
		CreateTableBean createTableBean = new CreateTableBean();
		createTableBean.setTableName(tableName);
		createTableBean.setColumnNames(Arrays.asList("col1", "col2", "col3", "col4"));
		createTableBean.setDataType(Arrays.asList("string", "string", "string", "string"));
		
		String json = HareSparkResourceTest.objectToJsonStr(createTableBean);
		StringRequestEntity requestEntity = new StringRequestEntity(json, contentType, charSet);
		method.setRequestEntity(requestEntity);
		client.executeMethod(method);
		byte[] resultByte = method.getResponseBody(1024 * 1024 * 10);
		String result = new String(resultByte);
		
		Gson gson = new Gson();
		return gson.fromJson(result, ResponseInfoBean.class);
	}

	private String dropTable(String tableName) throws Exception{
		
		PostMethod method = new PostMethod(httpUrlRoot + "/droptable");
		DropTableBean dropTableBean = new DropTableBean();
		dropTableBean.setTableName(tableName);
		
		String json = HareSparkResourceTest.objectToJsonStr(dropTableBean);
		StringRequestEntity requestEntity = new StringRequestEntity(json, contentType, charSet);
		method.setRequestEntity(requestEntity);
		
		client.executeMethod(method);
		
		byte[] resultByte = method.getResponseBody(1024 * 1024 * 10);
		String result = new String(resultByte);
		
		Gson gson = new Gson();
		ResponseInfoBean dropTable = gson.fromJson(result, ResponseInfoBean.class);
		return dropTable.getStatus();
	}

	private DescribeTableResponseBean tableDescribe(String tableName) throws Exception{
		PostMethod method = new PostMethod(httpUrlRoot + "/describetable");
		DescribeTableBean describeTableBean = new DescribeTableBean();
		describeTableBean.setTableName(tableName);
		String json = HareSparkResourceTest.objectToJsonStr(describeTableBean);

		StringRequestEntity requestEntity = new StringRequestEntity(json, contentType, charSet);
		method.setRequestEntity(requestEntity);
		
		int statusCode = client.executeMethod(method);
		assertEquals(200, statusCode);
		
		byte[] resultByte = method.getResponseBody(1024 * 1024 *10);
		String result = new String(resultByte);
		
		Gson gson = new Gson();
		return gson.fromJson(result, DescribeTableResponseBean.class);
	}
	
	private boolean isExists(String tableName) throws Exception{
		PostMethod method = new PostMethod(httpUrlRoot + "/isExists");
		
		StringRequestEntity requestEntity = new StringRequestEntity(tableName, contentType, charSet);
		method.setRequestEntity(requestEntity);
		client.executeMethod(method);
		byte[] resultByte = method.getResponseBody(1024 * 1024 * 10);
		String result = new String(resultByte);
		
		Gson gson = new Gson();
		if(gson.fromJson(result, String.class).equals("true")){
			return true;
		}else{
			return false;
		}
	}
	@Test
	public void test(){
		
		Gson gson = new Gson();
		String tableName = "table1111";
		QuerySubmitBean querySubmitBean = new QuerySubmitBean();
		querySubmitBean.setResultFileFolder("/tmp/table1111result");
		querySubmitBean.setTablename(tableName);
		querySubmitBean.setSql("SELECT * FROM " + tableName);
		
		
		System.out.println(gson.toJson(querySubmitBean));
		
		
	}
	
	private String uploadFile(String tableName) throws Exception {
		
		if(this.isExists(tableName)){
			this.dropTable(tableName);
		}
		this.createTable(tableName);
		
		PostMethod method = new PostMethod(httpUrlRoot + "/uploaddatafile");
		UploadDataFileBean uploadDataFileBean = new UploadDataFileBean();
		uploadDataFileBean.setTableName(tableName);
		uploadDataFileBean.setDataFilePath("/origin.txt");
		uploadDataFileBean.setSkipHeader("true");
		uploadDataFileBean.setCsvSeparator(",");
		uploadDataFileBean.setOperator("replace");
		uploadDataFileBean.setResultPath(clusterHost + "/resultstatus2");
		
		String json = HareSparkResourceTest.objectToJsonStr(uploadDataFileBean);
		
		StringRequestEntity requestEntity = new StringRequestEntity(json, contentType, charSet);
		method.setRequestEntity(requestEntity);
		client.executeMethod(method);
		
		byte[] resultByte = method.getResponseBody(1024 * 1024 * 10);
		String result = new String(resultByte);
		
		Gson gson = new Gson();
		UploadDataFileResponseBean response = gson.fromJson(result, UploadDataFileResponseBean.class);
		assertEquals("success", response.getStatus());
		
		return response.getUploadJobName();
	}
}
