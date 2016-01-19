package com.haredb.client.rest.resource;

import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.haredb.client.facade.bean.BulkloadStatusBean;
import com.haredb.client.facade.bean.ConnectionBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.TableInfoBean;
import com.haredb.client.facade.bean.UploadSchemaBean;
import com.haredb.client.facade.operator.HareBulkLoadDataBySchema;
import com.haredb.client.facade.operator.HareDefineHTable;
import com.haredb.client.util.ConnectionUtil;
import com.haredb.hbaseclient.core.Connection;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumHiveMetaStoreConnectType;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumSQLType;
import com.jayway.restassured.response.Response;

import com.jayway.restassured.response.ValidatableResponse;
//import static com.jayway.restassured.path.json.JsonPath.*;
import static com.jayway.restassured.RestAssured.*;
import static com.jayway.restassured.matcher.RestAssuredMatchers.*;
import static org.hamcrest.Matchers.*;

public class HareRestfulIntegratingTest {
	private static final Logger logger = LoggerFactory.getLogger(HareRestfulIntegratingTest.class);
	
	//rest base url
	private static final String BASERESTRUL = "http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi";
	private static final String HOST = "host1";
	private static final String TABLENAME = "HareRestTestTable_By_UnitTesting";
	
	private static final String _JAYWAY_CONNECTTYPE = "application/json; charset=UTF-8";
	private static final String _SUCCESS = "success"; 
	
	private static ConnectionBean connectionBean;
	private static Connection hareConnection;
	private static TableInfoBean tibn;
	private static UploadSchemaBean upsBean;
	private static String jobName;
	private static String sessionID;
	private static boolean isGenData = false;
	
	
	@BeforeClass
	public static void setUpBeforeClass() {
		try {
			//setting connection
			if(connectionBean == null) {
				connectionBean = new ConnectionBean();
				connectionBean.setZookeeperHost(HOST);
				connectionBean.setZookeeperPort("2181");
				connectionBean.setNameNodeHostPort("hdfs://"+ HOST +":8020");
				connectionBean.setRmAddressHostPort(HOST +":8032");
				connectionBean.setRmSchedulerAddressHostPort(HOST +":8030");
				connectionBean.setRmResourceTrackerAddressHostPort(HOST +":8031");
				connectionBean.setRmAdminAddressHostPort( HOST +":8033");
				connectionBean.setMrJobhistoryAddress(HOST +":10020");
				connectionBean.setHiveConnType(EnumHiveMetaStoreConnectType.LOCAL);
				connectionBean.setMetaStoreConnectURL("jdbc:mysql://192.168.1.215:3306/metastore_db");
				connectionBean.setMetaStoreConnectDriver("com.mysql.jdbc.Driver");
				connectionBean.setMetaStoreConnectUserName("root");
				connectionBean.setMetaStoreConnectPassword("123456");
				connectionBean.setDbName("default");
				connectionBean.setDbBrand(EnumSQLType.MYSQL);
				connectionBean.setEnableKerberos(false);
			}
			
			ConnectionUtil connUtil = new ConnectionUtil(connectionBean);
			connUtil.resolveBean();
			hareConnection = connUtil.getConn();
			
			//Setting table info
			tibn = new TableInfoBean();
			tibn.setTableName(TABLENAME);
			tibn.setColumnFamilys(Arrays.asList(new String[]{"cf1","cf2"}));
			
			//Creating table
			HareDefineHTable defineHTable = new HareDefineHTable(hareConnection);
			MessageInfo minfo = defineHTable.createHTable(tibn);
			if(minfo.getStatus() == MessageInfo.ERROR) {
				Assert.fail("Creating table fail @BeforeClass !");
			}
			
			
			//====generating data====
			upsBean = new UploadSchemaBean();
			upsBean.setDataPath("hdfs://host1:8020/restTable_bk1/bkfile.txt");
			upsBean.setResultPath("hdfs://host1:8020/tmp/restTable_bk1_exception.txt");
			upsBean.setSchemaFilePath("hdfs://host1:8020/restTable_bk1/bkfile_schema.schema");
			
			HareBulkLoadDataBySchema bulkloadData = new HareBulkLoadDataBySchema(hareConnection,upsBean);
			BulkloadStatusBean bReturn = bulkloadData.Bulkload();
			if(bReturn.getStatus() == MessageInfo.SUCCESS) {
				jobName = bReturn.getJobName();
				logger.debug("jobName = "+jobName);
			} else {
				Assert.fail("Bulkload data fail @BeforeClass !");
			}
		
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	@AfterClass
	public static void setUpAfterClass() {
		//drop table
		HareDefineHTable defineHTable = new HareDefineHTable(hareConnection);
		MessageInfo minfo = defineHTable.dropHTable(tibn.getTableName());
		if(minfo.getStatus() == MessageInfo.ERROR) {
			Assert.fail("Drop table fail @AfterClass !");
		}	
	}
	
	@Test
	public void testConnection(){
		Response connResponse = given().
				contentType(_JAYWAY_CONNECTTYPE).
				body(connectionBean).
		when().
				post(BASERESTRUL+"/connect");
		
		sessionID = connResponse.getSessionId();
		
		connResponse.
				then().
					statusCode(200).
					body("status", equalTo(_SUCCESS));
	}
	
	@Test
	public void testBulkloadStatus() {
		BulkloadStatusBean bsbn = new BulkloadStatusBean();
		bsbn.setJobName(jobName);
		
		given().
				sessionId(sessionID).
				contentType(_JAYWAY_CONNECTTYPE).
				body(bsbn).
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
	public void testBKloadWithNewThread(){
		given().
				sessionId(sessionID).
				contentType(_JAYWAY_CONNECTTYPE).
				body(upsBean).
		when().
				post(BASERESTRUL+"/bulkload/schema/upload").
		then().
				statusCode(200).
				body("status", equalTo(_SUCCESS)).
				body("jobName", is(notNullValue()));
	}
	
	@Test
	public void testBkloadWithQueue(){
		given().
				sessionId(sessionID).
				contentType(_JAYWAY_CONNECTTYPE).
				body(upsBean).
		when().
				post(BASERESTRUL+"/bulkload/schema/scheduledupload").
		then().
				statusCode(200).
				body("status", equalTo(_SUCCESS)).
				body("jobName", is(notNullValue()));
	}
	
	@Test
	public void testCreating_Altering_DropingTable() {
		
		TableInfoBean tbibn = new TableInfoBean();
		tbibn.setTableName("unitTestingTableCreateAndDrop");
		tbibn.setColumnFamilys(Arrays.asList(new String[]{"cfa","cfb"}));
		
		
		//create table
		given().
				sessionId(sessionID).
				contentType(_JAYWAY_CONNECTTYPE).
				body(tbibn).
		when().
				post(BASERESTRUL+"/hbaseadmin/create").
		then().
				statusCode(200).
				body("status", equalTo(_SUCCESS));
		
		//alter table
		
		
		//drop table
		given().
				sessionId(sessionID).
				contentType(_JAYWAY_CONNECTTYPE).
				body(tbibn).
		when().
				post(BASERESTRUL+"/hbaseadmin/drop").
		then().
				statusCode(200).
				body("status", equalTo(_SUCCESS));	
		
	}
}
