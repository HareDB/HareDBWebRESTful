package com.haredb.client.rest.resource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.io.Resources;
import com.haredb.client.facade.bean.BulkloadStatusBean;
import com.haredb.client.facade.bean.ConnectionBean;
import com.haredb.client.facade.bean.DataCellBean;
import com.haredb.client.facade.bean.HareQLQueryBean;
import com.haredb.client.facade.bean.IndexBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.MetaColumnFamilyBean;
import com.haredb.client.facade.bean.MetaTableBean;
import com.haredb.client.facade.bean.QueueBean;
import com.haredb.client.facade.bean.ScanConditionBean;
import com.haredb.client.facade.bean.TableInfoBean;
import com.haredb.client.facade.bean.UploadSchemaBean;
import com.haredb.client.facade.operator.HareBulkLoadDataBySchema;
import com.haredb.client.facade.operator.HareDefineHTable;
import com.haredb.client.util.ConnectionUtil;
import com.haredb.hbaseclient.core.Connection;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumHiveMetaStoreConnectType;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumSQLType;
import com.jayway.restassured.builder.ResponseSpecBuilder;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import com.jayway.restassured.specification.ResponseSpecification;

import static com.jayway.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;

public class HareRestfulIntegratingTest {
	private static final Logger logger = LoggerFactory.getLogger(HareRestfulIntegratingTest.class);
	
	//rest base url
	private static final String BASERESTRUL = "http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi";
	private static final String HOST = "host1";
	private static final String HDFSBASEURL = "hdfs://"+HOST+":8020";
	private static final String TABLENAME = "HareRestTestTable_By_UnitTesting";
	private static final String METATABLENAME = "HareRestTestTable_By_UnitTesting_m";
	private static final String TESTHTABLENAME = "HareRestTestTable_By_UnitTesting_Alter";
	private static final String CLOUMNFAMILY = "cfa";
	
	private static final String bkFilePath="/"+ TABLENAME +"/bkfile.txt";
	private static final String bkSchemaPath="/"+ TABLENAME +"/bkfile_schema.schema";
	private static final String bkHDFSPath="/"+ TABLENAME;
	private static final String tempFolderRoot = "/temp/"+TABLENAME;
	private static final String bkFileName = "bkfile.txt";
	private static final String schemaFileName = "bkfile_schema.schema";
	
	private static final String _JAYWAY_CONNECTTYPE = "application/json; charset=UTF-8";
	private static final String _SUCCESS = "success"; 
	
	
	private static ResponseSpecification thenResponseSpec;
	
	private static ConnectionBean connectionBean;
	private static Connection hareConnection;
	private static TableInfoBean tibn;
	private static UploadSchemaBean upsBean;
	private static IndexBean ixBean;
	private static QueueBean quBean;
	private static String jobName;
	private static String sessionID;
	
	private static enum RESTTYPE { POST,DELETE,GET,PUT }
	
	@BeforeClass
	public static void setUpBeforeClass() {
		//testing expect setting
		ResponseSpecBuilder resBuilder = new ResponseSpecBuilder();
		resBuilder.expectStatusCode(200);
		resBuilder.expectBody("status", equalTo(_SUCCESS));
		thenResponseSpec = resBuilder.build();
		
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
			tibn.setColumnFamilys(Arrays.asList(new String[]{"cf","cf1",CLOUMNFAMILY}));
			
			//Setting Index bean
			ixBean = new IndexBean();
			ixBean.setTableName(tibn.getTableName());
			ixBean.setCollectionList(tibn.getColumnFamilys().get(0)+":column1");
			
			//Setting Queue Bean
			quBean = new QueueBean();
			quBean.setTableName(tibn.getTableName());
			
			//Creating table
			HareDefineHTable defineHTable = new HareDefineHTable(hareConnection);
			MessageInfo minfo = defineHTable.createHTable(tibn);
			if(minfo.getStatus() == MessageInfo.ERROR) {
				Assert.fail("Creating table fail @BeforeClass !");
			}
			
			/*upload file to HDFS*/
			//overwrite schema file
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			File bkFiles = new File(classLoader.getResource("bkdata/"+ bkFileName).getFile());
			String text = Resources.toString(new File(classLoader.getResource("bkdata/"+schemaFileName).getFile()).toURL(), Charsets.UTF_8);
			text = text.replace("@tablename", TABLENAME).replace("@outputfolder", connectionBean.getNameNodeHostPort()+tempFolderRoot+"/"+TABLENAME+"_prebkload");
			
			FileSystem hdfs=null;
			hdfs = FileSystem.get(hareConnection.getConfig());
			Path schemaPath = new Path(connectionBean.getNameNodeHostPort()+bkSchemaPath);
			if (hdfs.exists(schemaPath)) {
				hdfs.delete(schemaPath, true);
			}
			FSDataOutputStream fsOutStream = hdfs.create(schemaPath);
			fsOutStream.write(text.getBytes(),0, text.getBytes().length);
			hdfs.copyFromLocalFile(new Path(bkFiles.getPath()), new Path(bkHDFSPath));
			fsOutStream.close();
			hdfs.close();
			
			//====generating data====
			upsBean = new UploadSchemaBean();
			upsBean.setDataPath(HDFSBASEURL + bkFilePath);
			upsBean.setResultPath(HDFSBASEURL+"/tmp/"+ TABLENAME +"_exception.txt");
			upsBean.setSchemaFilePath(HDFSBASEURL + bkSchemaPath);
			
			HareBulkLoadDataBySchema bulkloadData = new HareBulkLoadDataBySchema(hareConnection,upsBean);
			BulkloadStatusBean bReturn = bulkloadData.Bulkload();
			if(bReturn.getStatus() == MessageInfo.SUCCESS) {
				jobName = bReturn.getJobName();
				logger.debug("jobName = "+jobName);
			} else {
				Assert.fail("Bulkload data fail @BeforeClass !");
			}
		
			//get session id
			Response connResponse = getResponse("/connect" ,connectionBean, RESTTYPE.POST);
			sessionID = connResponse.getSessionId();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	@AfterClass
	public static void setUpAfterClass() throws IOException {
		/*delete all test file*/
		//delete bkfiles
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(hareConnection.getConfig());
			hdfs.delete(new Path(bkHDFSPath), true);
			hdfs.delete(new Path(tempFolderRoot), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			hdfs.close();
		}
		
		//drop table
		HareDefineHTable defineHTable = new HareDefineHTable(hareConnection);
		MessageInfo minfo = defineHTable.dropHTable(tibn.getTableName());
		if(minfo.getStatus() == MessageInfo.ERROR) {
			Assert.fail("Drop table ["+tibn.getTableName()+"] fail @AfterClass !");
		}
		minfo = defineHTable.dropHTable(TESTHTABLENAME);
		if(minfo.getStatus() == MessageInfo.ERROR) {
			Assert.fail("Drop table ["+TESTHTABLENAME+"] fail @AfterClass !");
		}
	}
	
	private static Response getResponse(String restUri,Object object,RESTTYPE type) {
		RequestSpecification speci =
		given().
				contentType(_JAYWAY_CONNECTTYPE);
		
		if(object != null) {
			speci.body(object);
		}
		
		if(sessionID != null) {
			speci.sessionId(sessionID);
		}
		
		Response response = null;
		
		if(type == RESTTYPE.GET) {
			response = speci.when().get(BASERESTRUL+restUri);
		} else if (type == RESTTYPE.DELETE) {
			response = speci.when().delete(BASERESTRUL+restUri);
		} else if (type == RESTTYPE.POST) {
			response = speci.when().post(BASERESTRUL+restUri);
		} else if (type == RESTTYPE.PUT) {
			response = speci.when().put(BASERESTRUL+restUri);
		}
		
		return response;
	}
	
	@Test
	public void testBulkloadStatus() {
		BulkloadStatusBean bsbn = new BulkloadStatusBean();
		bsbn.setJobName(jobName);
		
		getResponse("/bulkload/status" ,bsbn, RESTTYPE.POST).
		then().
				spec(thenResponseSpec).
				body("bulkloadFinishTime", is(notNullValue())).
				body("bulkloadStartTime", is(notNullValue())).
				body("jobId", is(notNullValue())).
				body("jobName", equalTo(jobName)).
				body("jobStatus", equalTo("SUCCEEDED"));
		
	}
	
	@Test
	public void testBKloadWithNewThread(){
		getResponse("/bulkload/schema/upload" ,upsBean, RESTTYPE.POST).
		then().
				spec(thenResponseSpec).
				body("jobName", is(notNullValue()));

	}
	
	@Test
	public void testBkloadWithQueue(){
		getResponse("/bulkload/schema/scheduledupload" ,upsBean, RESTTYPE.POST).
		then().
				spec(thenResponseSpec).
				body("jobName", is(notNullValue()));
	}
	
	@Test
	public void testCreating_Altering_DropingTable() {
		TableInfoBean tbibn = new TableInfoBean();
		tbibn.setTableName(TESTHTABLENAME);
		tbibn.setColumnFamilys(Arrays.asList(new String[]{"cfa","cfb"}));
		
		
		//create table
		getResponse("/hbaseadmin/create" ,tbibn, RESTTYPE.POST).
		then().
				spec(thenResponseSpec);
		
		//alter table
		MetaColumnFamilyBean mcbn = new MetaColumnFamilyBean();
		mcbn.setColumnFamilyName("cfa");
		mcbn.setVersions(1);
		
		getResponse("/hbaseadmin/alter/"+TESTHTABLENAME ,mcbn, RESTTYPE.PUT).
		then().
				spec(thenResponseSpec);
		
		//drop table
		getResponse("/hbaseadmin/drop" ,tbibn, RESTTYPE.POST).
		then().
				spec(thenResponseSpec);
		
	}
	
	@Test
	public void testMetaTable() {
		//Create meta table
		MetaTableBean mtbn = new MetaTableBean();
		mtbn.setHbaseTableName(TABLENAME);
		mtbn.setMetaTableName(METATABLENAME);
		mtbn.setMetaColumnNames(Arrays.asList(new String[]{"rowkey","col1","col2","col3","col4","col5","col6"}));
		mtbn.setHbaseColumnNames(Arrays.asList(new String[]{":key","cf:column1","cf:column2","cf:column3","cf:column4","cf:column5","cf:column6"}));
		mtbn.setDataTypes(Arrays.asList(new String[]{"STRING","STRING","STRING","STRING","STRING","STRING","STRING"}));
		
		getResponse("/haremeta/create" ,mtbn, RESTTYPE.POST).
		then().
				spec(thenResponseSpec);
		
		//Alter
		mtbn.setMetaColumnNames(Arrays.asList(new String[]{"col1","col2","col3","rowkey"}));
		mtbn.setHbaseColumnNames(Arrays.asList(new String[]{"cf:column1","cf:column2","cf:column3",":key"}));
		mtbn.setDataTypes(Arrays.asList(new String[]{"STRING","STRING","STRING","STRING"}));
		
		getResponse("/haremeta/alter" ,mtbn, RESTTYPE.POST).
		then().
				spec(thenResponseSpec);
		
		//Describing
		getResponse("/haremeta/describe" ,mtbn, RESTTYPE.POST).
		then().
				spec(thenResponseSpec).
				body("hbaseTableName", equalTo(TABLENAME)).
				body("dataTypes", hasItems("string","string","string","string")).
				body("hbaseColumnNames", hasItems("cf:column1","cf:column2","cf:column3",":key"));
		
		//Drop
		getResponse("/haremeta/drop" ,mtbn, RESTTYPE.POST).
		then().
				spec(thenResponseSpec);
		
	}
	
	@Test
	public void testHareQL() {
		HareQLQueryBean hqbn = new HareQLQueryBean();
		hqbn.setTempFilePath(tempFolderRoot+"/"+TABLENAME+"_query");
		hqbn.setSql("select * from "+TABLENAME);
		hqbn.setPage(1);
		hqbn.setLimit(10);
		
		getResponse("/hareql/query" ,hqbn, RESTTYPE.POST).
		then().
				spec(thenResponseSpec).
				body("fileSize", greaterThan("0")).
				body("rowSize", greaterThan("0"));
	}
	
	@Test
	public void testHareQLSubmit() {	
		HareQLQueryBean hqbn = new HareQLQueryBean();
		hqbn.setTempFilePath(tempFolderRoot+"/"+TABLENAME+"_submitquery");
		hqbn.setSql("select * from "+TABLENAME);
		hqbn.setPage(1);
		hqbn.setLimit(10);
		
		getResponse("/hareql/submit" ,hqbn, RESTTYPE.POST).
		then().
				spec(thenResponseSpec);
	}
	
	@Test
	public void testHareQLStatus() {
		HareQLQueryBean hqbn = new HareQLQueryBean();
		hqbn.setTempFilePath(tempFolderRoot+"/"+TABLENAME+"_status");
		getResponse("/hareql/status" ,hqbn, RESTTYPE.POST).
		then().
		statusCode(200);
	}
	
	@Test
	public void testScanTable() {
		ScanConditionBean scb = new ScanConditionBean();
		scb.setTableName(TABLENAME);
		scb.setPageSize(1);
		scb.setLimit(10);
		
		getResponse("/htable/scan" ,scb, RESTTYPE.POST).
		then().
				spec(thenResponseSpec).
				body("heads.rowkey", hasItem(":key")).
				body("heads.columnFamily", hasItem(tibn.getColumnFamilys().get(0))).
				body("heads.qualifier", hasItems("column1","column2","column3","column4","column5","column6")).
				body("results.item.size()", greaterThan(0));
	}
	
	@Test
	public void testPut() {
		DataCellBean dcbn = new DataCellBean();
		dcbn.setTableName(TABLENAME);
		dcbn.setRowkey("087987");
		dcbn.setColumnFamily(CLOUMNFAMILY);
		dcbn.setQualifier("cola");
		dcbn.setValue("putdata");
		
		getResponse("/htable/put" ,dcbn, RESTTYPE.POST).
		then().
				spec(thenResponseSpec);
	}
	
	@Test
	public void testDeleteHtableCellRecord() {
		getResponse("/htable/delete/"+TABLENAME+"/087987/"+CLOUMNFAMILY+"/cola" ,null, RESTTYPE.DELETE).
		then().
				spec(thenResponseSpec);
	}
	
	@Test
	public void testDeleteHtableColumnFamilyRecorder() {
		getResponse("/htable/delete/"+TABLENAME+"/087987/"+CLOUMNFAMILY ,null, RESTTYPE.DELETE).
		then().
				spec(thenResponseSpec);
	}
	
	@Test
	public void testDeleteHtableRowkeyRecorder() {
		getResponse("/htable/delete/"+TABLENAME+"/087987" ,null, RESTTYPE.DELETE).
		then().
				spec(thenResponseSpec);
	}
	
	@Test
	public void testCreateIndex() {
		ixBean.setResultPath(HDFSBASEURL+tempFolderRoot+"/"+TABLENAME+"_index_create.txt");
		
		getResponse("/index/schema/createindex" ,ixBean, RESTTYPE.POST).
		then().
				spec(thenResponseSpec);
	}
	
	@Test
	public void testUpdateIndex() {
		ixBean.setResultPath(HDFSBASEURL+tempFolderRoot+"/"+TABLENAME+"_index_update.txt");
		
		getResponse("/index/schema/updateindex" ,ixBean, RESTTYPE.POST).
		then().
				spec(thenResponseSpec);
	}
	
	@Test
	public void testDropIndex() {
		ixBean.setResultPath(HDFSBASEURL+tempFolderRoot+"/"+TABLENAME+"_index_drop.txt");
		
		getResponse("/index/schema/drop" ,ixBean, RESTTYPE.POST).
		then().
				spec(thenResponseSpec);
	}
	
	@Test
	public void testQueueGetStatus() {
		quBean.setResultPath(HDFSBASEURL+tempFolderRoot+"/"+TABLENAME+"_queue_status.txt");
		
		getResponse("/jobqueue/schema/status" ,quBean, RESTTYPE.POST).
		then().
				spec(thenResponseSpec).
				body("tableName", equalTo(tibn.getTableName())).
				body("queueStatus", is(notNullValue()));
	}
	
	@Test
	public void testQueueGetQueueFile() {
		quBean.setResultPath(HDFSBASEURL+tempFolderRoot+"/"+TABLENAME+"_queue_getQueueFile.txt");
		
		getResponse("/jobqueue/schema/getQueueFile" ,quBean, RESTTYPE.POST).
		then().
				spec(thenResponseSpec).
				body("tableName", equalTo(tibn.getTableName()));
	}
	
	@Test
	public void testQueueDeleteQueueJob() {
		quBean.setResultPath(HDFSBASEURL+tempFolderRoot+"/"+TABLENAME+"_queue_deleteQueueJob.txt");
		quBean.setQueueFileName(HDFSBASEURL+"/haredb/jobque/restTable_bk1/1452676302481_Bulkload");
		
		getResponse("/jobqueue/schema/deleteJob" ,quBean, RESTTYPE.POST).
		then().
				spec(thenResponseSpec).
				body("tableName", equalTo(tibn.getTableName()));
		
	}
	
	@Test
	public void testQueueDropQueue() {
		quBean.setResultPath(HDFSBASEURL+tempFolderRoot+"/"+TABLENAME+"_queue_dropQueue.txt");
		
		getResponse("/jobqueue/schema/dropQueue" ,quBean, RESTTYPE.POST).
		then().
				spec(thenResponseSpec).
				body("tableName", equalTo(tibn.getTableName()));
	}
	
	@Test
	public void testQueueFroceCompelect() {
		quBean.setResultPath(HDFSBASEURL+tempFolderRoot+"/"+TABLENAME+"_queue_froceCompelect.txt");
		
		getResponse("/jobqueue/schema/froceCompelect" ,quBean, RESTTYPE.POST).
		then().
				spec(thenResponseSpec).
				body("tableName", equalTo(tibn.getTableName())).
				body("queueStatus", equalTo("Complete"));
	}
	
	@Test
	public void testQueueRestart() {
		quBean.setResultPath(HDFSBASEURL+tempFolderRoot+"/"+TABLENAME+"_queue_queueRestart.txt");
		
		getResponse("/jobqueue/schema/restart" ,quBean, RESTTYPE.POST).
		then().
				spec(thenResponseSpec).
				body("tableName", equalTo(tibn.getTableName()));
	}
}
