package com.haredb.client.rest.resource;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.haredb.harespark.bean.input.AlterTableBean;
import com.haredb.harespark.bean.input.CreateTableBean;
import com.haredb.harespark.bean.input.DescribeTableBean;
import com.haredb.harespark.bean.input.DropTableBean;
import com.haredb.harespark.bean.input.UserSessionBean;
import com.haredb.harespark.bean.response.DescribeTableResponseBean;
import com.haredb.harespark.bean.response.ResponseInfoBean;


public class HareSparkResourceTest{

	private static HttpClient client = new HttpClient();
	private static String httpUrlRoot = "http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/harespark";
	private static String contentType = "application/json";
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

	private static String objectToJsonStr(Object object){
		Gson gson = new Gson();
		return gson.toJson(object);
	}
	
	private static UserSessionBean getUserSession(){
		UserSessionBean userSessionBean = new UserSessionBean();
		userSessionBean.setConfigurationFolderPath("/home/user1/hadoop4");
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
		
		int statusCode = client.executeMethod(method);
		
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
}
