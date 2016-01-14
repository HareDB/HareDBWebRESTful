package com.haredb.client.rest.resource;

import java.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.haredb.client.facade.bean.BulkloadStatusBean;
import com.haredb.client.facade.bean.ConnectionBean;
import com.haredb.client.facade.bean.TableInfoBean;
import com.haredb.client.facade.bean.UploadSchemaBean;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumHiveMetaStoreConnectType;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumSQLType;

import com.jayway.restassured.response.Response;
import static com.jayway.restassured.path.json.JsonPath.*;
import static com.jayway.restassured.RestAssured.*;
import static com.jayway.restassured.matcher.RestAssuredMatchers.*;

import static org.hamcrest.Matchers.*;

public class HareRestfulIntegratingTest {

//	private static final Logger logger = LoggerFactory.getLogger(HareRestfulIntegratingTest.class);
	
	//rest base url
	private static final String BASERESTRUL = "http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi";
	private static final String HOST = "host1";
	private static final String TABLENAME = "HareRestTestTable_By_UnitTesting";
	
	private static final String _JAYWAY_CONNECTTYPE = "application/json; charset=UTF-8";
	private static final String _SUCCESS = "success"; 
	
	private static ConnectionBean connection;
	private static TableInfoBean tibn;
	private static String jobName;
	private static boolean isGenData = false;
	
	
	@BeforeClass
	public static void setUpBeforeClass() {
	
		//setting connection
		if(connection == null) {
			connection = new ConnectionBean();
			connection.setZookeeperHost(HOST);
			connection.setZookeeperPort("2181");
			connection.setNameNodeHostPort("hdfs://"+ HOST +":8020");
			connection.setRmAddressHostPort(HOST +":8032");
			connection.setRmSchedulerAddressHostPort(HOST +":8030");
			connection.setRmResourceTrackerAddressHostPort(HOST +":8031");
			connection.setRmAdminAddressHostPort( HOST +":8033");
			connection.setMrJobhistoryAddress(HOST +":10020");
			connection.setHiveConnType(EnumHiveMetaStoreConnectType.LOCAL);
			connection.setMetaStoreConnectURL("jdbc:mysql://192.168.1.215:3306/metastore_db");
			connection.setMetaStoreConnectDriver("com.mysql.jdbc.Driver");
			connection.setMetaStoreConnectUserName("root");
			connection.setMetaStoreConnectPassword("123456");
			connection.setDbName("default");
			connection.setDbBrand(EnumSQLType.MYSQL);
			connection.setEnableKerberos(false);
		}
		
		//connect
		/*
		given().
				contentType(_JAYWAY_CONNECTTYPE).
				body(connection).
		when().
				post(BASERESTRUL+"/connect").
		then().
				statusCode(200).
				body("status", equalTo(_SUCCESS));
		*/
		
		//setting table info
		tibn = new TableInfoBean();
		tibn.setTableName(TABLENAME);
		tibn.setColumnFamilys(Arrays.asList(new String[]{"cf1","cf2"}));
		/*
		given().
				contentType(_JAYWAY_CONNECTTYPE).
				body(tibn).
		when().
				post(BASERESTRUL+"/hbaseadmin/create").
		then().
				statusCode(200).
				body("status", equalTo(_SUCCESS));
		*/
		
		//====generating data====
		UploadSchemaBean usbn = new UploadSchemaBean();
		usbn.setDataPath("hdfs://host1:8020/restTable_bk1/bkfile.txt");
		usbn.setResultPath("hdfs://host1:8020/tmp/restTable_bk1_exception.txt");
		usbn.setSchemaFilePath("hdfs://host1:8020/restTable_bk1/bkfile_schema.schema");
		
		Response gendata = 
			given().
					contentType(_JAYWAY_CONNECTTYPE).
					body(usbn).
			when().
					post(BASERESTRUL+"/bulkload/schema/upload");
		
		// valid
		gendata.
			then().
					statusCode(200).
					body("status", equalTo(_SUCCESS)).
					body("jobName", is(notNullValue()));
		// set job name
		jobName = 
				with(gendata.asString()).get("jobName");
	}

	@AfterClass
	public static void setUpAfterClass() {
		//drop data
		given().
				contentType(_JAYWAY_CONNECTTYPE).
				body(tibn).
		when().
				post(BASERESTRUL+"/hbaseadmin/drop").
		then().
				statusCode(200).
				body("status", equalTo(_SUCCESS));		
	}
	
	@Test
	public void testBulkloadStatus() {
		BulkloadStatusBean bsbn = new BulkloadStatusBean();
		bsbn.setJobName(jobName);
		
		given().
				contentType(_JAYWAY_CONNECTTYPE).
				body(tibn).
		when().
				post(BASERESTRUL+"/bulkload/status").
		then().
				statusCode(200).
				body("status", equalTo(_SUCCESS)).
				body("bulkloadFinishTime", is(notNullValue())).
				body("bulkloadStartTime", is(notNullValue())).
				body("jobId", is(notNullValue())).
				body("jobName", equalTo(jobName)).
				body("jobStatus", equalTo("SUCCEEDED"));
	}
	
	@Test
	public void testConnection(){
		given().
				contentType(_JAYWAY_CONNECTTYPE).
				body(connection).
		when().
				post(BASERESTRUL+"/connect").
		then().
				statusCode(200).
				body("status", equalTo(_SUCCESS));
	}
	
	@Test
	public void testCreatingAndDropingTable() {
		
		TableInfoBean tbibn = new TableInfoBean();
		tbibn.setTableName("unitTestingTableCreateAndDrop");
		tbibn.setColumnFamilys(Arrays.asList(new String[]{"cfa","cfb"}));
		
		
		//create table
		given().
				contentType(_JAYWAY_CONNECTTYPE).
				body(tbibn).
		when().
				post(BASERESTRUL+"/hbaseadmin/create").
		then().
				statusCode(200).
				body("status", equalTo(_SUCCESS));
		
		//drop table
		given().
				contentType(_JAYWAY_CONNECTTYPE).
				body(tbibn).
		when().
				post(BASERESTRUL+"/hbaseadmin/drop").
		then().
				statusCode(200).
				body("status", equalTo(_SUCCESS));	
		
	}
}
