package com.haredb.client.rest.resource;

import static com.jayway.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.haredb.harespark.bean.input.AlterTableBean;
import com.haredb.harespark.bean.input.CreateTableBean;
import com.haredb.harespark.bean.input.DeleteDataFileBean;
import com.haredb.harespark.bean.input.DescribeTableBean;
import com.haredb.harespark.bean.input.DropTableBean;
import com.haredb.harespark.bean.input.PreviewBean;
import com.haredb.harespark.bean.input.QuerySubmitBean;
import com.haredb.harespark.bean.input.UploadDataFileBean;
import com.haredb.harespark.bean.input.UserSessionBean;
import com.jayway.restassured.response.Response;

public class HareRestfulIntegratingSparkTest {
private static final Logger logger = LoggerFactory.getLogger(HareRestfulIntegratingTest.class);
	
	//rest base url
	private static final String BASERESTRUL = "http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/harespark";
	private static String CLUSTERHOST = "hdfs://host1:8020";
	private static final String TABLENAME = "HareSparkRestTest";
	
	private static final String _JAYWAY_CONNECTTYPE = "application/json; charset=UTF-8";
	private static final String _SUCCESS = "success"; 
	private static String sessionId;
	
	
	@Before
	public void before() {
		checkLocalJarFile();
		checkSparkAssemblyJar();
		
		String clusterConfiguration = HareSparkResourceTest.class.getResource("/hadoopConf").getPath();
		UserSessionBean userSessionBean = new UserSessionBean();
		userSessionBean.setConfigurationFolderPath(clusterConfiguration);
		
		Response userSessionResponse = given().
				contentType(_JAYWAY_CONNECTTYPE).
				body(userSessionBean).
		when().
				post(BASERESTRUL+"/usersession");
		
		sessionId = userSessionResponse.getSessionId();
		if(this.isExistsTable()){
			this.dropTable();
		}
	    this.createTable();
	    this.uploadDataFile();
		
		
	}	
	@Test
	public void testCreateHareSparkTable(){
		if(this.isExistsTable()){
			this.dropTable();
		}
		this.createTable();
	}
	@Test
	public void testAlterHareSparkTable(){
		this.alterTable();
	}
	@Test
	public void testDropHareSparkTable(){
		this.dropTable();
	}
	@Test
	public void testIsExistsHareSparkTable(){
		assertTrue(isExistsTable());
	}	
	@Test
	public void testDescribeHareSparkTable(){
		this.describeTable();
	}	
	@Test
	public void testUploadDataFile(){
		this.uploadDataFile();
	}	
	@Test
	public void testQueryHareSparkTable(){
		this.submitQueryHareSpark();	
	}	
	@Test
	public void testDeleteFile(){
		
		this.deleteFile();
	}
	@Test
	public void testPreviewHareSparkTable(){
		this.previewTable();
	}
	
	
	
	private void createTable(){
		CreateTableBean createTableBean = new CreateTableBean();
		createTableBean.setTablename(TABLENAME);
		createTableBean.setColumnNames(Arrays.asList("col1", "col2", "col3", "col4"));
		createTableBean.setDataTypes(Arrays.asList("string", "string", "string", "string"));
		
		Response createHareSparkTableResponse = given().
				sessionId(sessionId).
				contentType(_JAYWAY_CONNECTTYPE).
				body(createTableBean).
		when().
				post(BASERESTRUL+"/createtable");
		
		
		createHareSparkTableResponse.
		        then().
			        statusCode(200).
			        body("status", equalTo(_SUCCESS));
	}
	private void alterTable(){
		AlterTableBean alterTableBean = new AlterTableBean();
		alterTableBean.setTablename(TABLENAME);
		alterTableBean.setColumnNames(Arrays.asList("col10", "col11", "col12", "col13"));
		alterTableBean.setDataTypes(Arrays.asList("string", "string", "string", "string"));
		
		Response alterHareSparkTableResponse = given().
				sessionId(sessionId).
				contentType(_JAYWAY_CONNECTTYPE).
				body(alterTableBean).
		when().
				post(BASERESTRUL+"/altertable");
		
		
		alterHareSparkTableResponse.
		        then().
			        statusCode(200).
			        body("status", equalTo(_SUCCESS));
		
	}
	private void dropTable(){
		DropTableBean dropTableBean = new DropTableBean();
		dropTableBean.setTablename(TABLENAME);
		
		Response dropHareSparkTableResponse = given().
				sessionId(sessionId).
				contentType(_JAYWAY_CONNECTTYPE).
				body(dropTableBean).
		when().
			    post(BASERESTRUL+"/droptable");
	
		dropHareSparkTableResponse.
        then().
	        statusCode(200).
	        body("status", equalTo(_SUCCESS));
	}
	private boolean isExistsTable(){
		Response isExistsHareSparkTable = given().
				sessionId(sessionId).
				contentType(_JAYWAY_CONNECTTYPE).
				body(TABLENAME).
		when().
				post(BASERESTRUL+"/isExists");
		
		isExistsHareSparkTable.
		then().
			statusCode(200);
		
		String result = isExistsHareSparkTable.asString();
		return Boolean.valueOf(result);
	}
	private void describeTable(){
		DescribeTableBean describeTableBean = new DescribeTableBean();
		describeTableBean.setTablename(TABLENAME);
		
		Response describeHareSparkTableResponse = given().
				sessionId(sessionId).
				contentType(_JAYWAY_CONNECTTYPE).
				body(describeTableBean).
		when().
				post(BASERESTRUL+"/describetable");
		
		
		describeHareSparkTableResponse.
		        then().
			        statusCode(200).
			        body("status", equalTo(_SUCCESS));
		
		List<String> columnNames = describeHareSparkTableResponse.jsonPath().get("columnNames");
		assertEquals(4, columnNames.size());
		List<String> columnDataType = describeHareSparkTableResponse.jsonPath().get("dataTypes");
		assertEquals(4, columnDataType.size());
	}
	private void uploadDataFile(){
		try{
			UploadDataFileBean uploadDataFileBean = new UploadDataFileBean();
			uploadDataFileBean.setTablename(TABLENAME);
			uploadDataFileBean.setDataFilePath("/origin.txt");
			uploadDataFileBean.setSkipHeader("true");
			uploadDataFileBean.setCsvSeparator(",");
			uploadDataFileBean.setOperator("replace");
			uploadDataFileBean.setResultPath(CLUSTERHOST + "/resultstatus2");
			
			Response uploadDataFileResponse = given().
					sessionId(sessionId).
					contentType(_JAYWAY_CONNECTTYPE).
					body(uploadDataFileBean).
			when().
					post(BASERESTRUL+"/uploaddatafile");
			
			uploadDataFileResponse.
				then().
				   statusCode(200).
				   body("uploadJobName", equalTo("hdfs://host1:8020/resultstatus2"));
			Thread.sleep(5000);
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
	private void submitQueryHareSpark(){
		String timeStamp = String.valueOf(System.currentTimeMillis());
		
		String tempFilePathAndName = "/tmp/queryresult" + timeStamp;
		QuerySubmitBean querySubmitBean = new QuerySubmitBean();
		querySubmitBean.setResultFileFolder(tempFilePathAndName);
		querySubmitBean.setTablename(TABLENAME);
		querySubmitBean.setSql("SELECT * FROM " + TABLENAME);
	
		Response querySubmitResponse = given().
				sessionId(sessionId).
				contentType(_JAYWAY_CONNECTTYPE).
				body(querySubmitBean).
		when().
				post(BASERESTRUL+"/querysubmit");
		
		querySubmitResponse.
        then().
	        statusCode(200).
	        body("status", equalTo(_SUCCESS));
	}
	private void previewTable(){
		PreviewBean previewBean = new PreviewBean();
		previewBean.setLimit("10");
		previewBean.setPageSize("1");
		previewBean.setTablename(TABLENAME);
		
		Response previewResponse = given().
				sessionId(sessionId).
				contentType(_JAYWAY_CONNECTTYPE).
				body(previewBean).
		when().
				post(BASERESTRUL+"/preview");
		
		List<String> results = previewResponse.jsonPath().get("results");
		assertEquals(5, results.size());
	}
	private void deleteFile(){
		DeleteDataFileBean deleteDataFileBean = new DeleteDataFileBean();
		deleteDataFileBean.setTablename(TABLENAME);
		deleteDataFileBean.setDeleteDataFileName("origin.txt");
		
		Response deleteFileResponse = given().
				sessionId(sessionId).
				contentType(_JAYWAY_CONNECTTYPE).
				body(deleteDataFileBean).
		when().
				post(BASERESTRUL+"/deletedatafile");
		
		deleteFileResponse.
		then().
			statusCode(200).
			body("status", equalTo(_SUCCESS));
	}
	private static void checkLocalJarFile(){
		File commonsJar = new File("build/lib/commons-csv-1.2.jar");
		File haresparkJar = new File("build/lib/commons-csv-1.2.jar");
		File sparkCsvJar = new File("build/lib/spark-csv_2.10-1.2.0.jar");
		if(!commonsJar.exists() || !haresparkJar.exists() || !sparkCsvJar.exists()){
			throw new RuntimeException("Please run \"gradle clean build -x test\" command");
		}
	}
	private static void checkSparkAssemblyJar(){
		try{
			InputStream inStream = HareSparkResourceTest.class.getResourceAsStream("/sysconfig.properties");
			Properties properties = new Properties();
			properties.load(inStream);
			String assemblyJarPath = properties.getProperty("sparkAssemblyJarPath");
			if(assemblyJarPath != null){
				Configuration config = new Configuration();
				config.set("fs.defaultFS", CLUSTERHOST);
				FileSystem fs = FileSystem.get(config);
				if(!fs.exists(new Path(assemblyJarPath))){
				    throw new RuntimeException(assemblyJarPath + " not exist spark assembly jar file, please go to the http://spark.apache.org/downloads.html download");	
				}
			}else{
				throw new RuntimeException("sysconfig.properties file not set sparkAssemblyJarPath value");
			}
			
			
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
