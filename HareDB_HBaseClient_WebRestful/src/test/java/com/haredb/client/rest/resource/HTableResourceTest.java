package com.haredb.client.rest.resource;



import static org.junit.Assert.assertEquals;

import javax.ws.rs.core.MediaType;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.haredb.client.facade.bean.ConnectionBean;
import com.haredb.client.facade.bean.DataCellBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.ScanResultStatusBean;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import com.sun.jersey.test.framework.spi.container.grizzly2.web.GrizzlyWebTestContainerFactory;

public class HTableResourceTest extends JerseyTest{
	private static final String BASEURL = "http://192.168.1.16:8080";
	
	@Test
	public void testScan(){
		WebResource webResource = client().resource(BASEURL);
		String tableName = "table81";
		String path = "/HareDB_HBaseClient_WebRestful/webapi/htable/scan/" + tableName + "/1/3";
		WebResource resource = webResource.path(path);
		ScanResultStatusBean result = resource.type(MediaType.APPLICATION_JSON).get(ScanResultStatusBean.class);
		int size = result.getHeads().size();
		String [][] resultSet = result.getResults();
		for(int i = 0 ; i < resultSet.length ; i++){
			for(int j = 0 ; j < size ; j++){
				System.out.print(resultSet[i][j] + "   ");
			}
			System.out.println("");
		}
	}
	@Test
	public void genGson(){
		Gson gson = new Gson();
		
		ConnectionBean connection = new ConnectionBean();
		connection.setConnectionName("connectionName");
		connection.setZookeeperHost("host1");
		connection.setZookeeperHost("2181");
		connection.setRmAddressHostPort("host1:8030");
		connection.setRmAdminAddressHostPort("host1:8034");
		connection.setRmResourceTrackerAddressHostPort("hoste1:8031");
		connection.setRmResourceTrackerAddressHostPort("host1:8035");
		connection.setNameNodeHostPort("hdfs://host1:9000");
		connection.setMetaStoreConnectDriver("com.mysql.jdbc.Driver");
		connection.setMetaStoreConnectURL("jdbc:mysql://192.168.1.215:3306/metastore_db");
		connection.setMetaStoreConnectUserName("user1");
		connection.setMetaStoreConnectPassword("111111");
		
		String input = gson.toJson(connection);
		System.out.println(input);
	}
	
	@Test
	public void testPut(){
		WebResource webResource = client().resource(BASEURL);
		String path = "/HareDB_HBaseClient_WebRestful/webapi/htable/put";
		
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		String input = gson.toJson(getPutDataCellBeanInstance());

		WebResource resource = webResource.path(path);
		MessageInfo result = resource.type(MediaType.APPLICATION_JSON).post(MessageInfo.class, input);
		assertEquals("success", result.getStatus());
	}
	@Test
	public void testDeleteRowKey(){
		String path = "/HareDB_HBaseClient_WebRestful/webapi/htable/delete/table95/rowkey1";
		this.testDelete(path);
	}
	@Test
	public void testDeleteRowKeyAndColumnFamily(){
		String path = "/HareDB_HBaseClient_WebRestful/webapi/htable/delete/table95/rowkey1/cf";
		this.testDelete(path);;
	}
	@Test
	public void testDeleteRowkeyAndColumnFamilyAndQualifier(){
		String path = "/HareDB_HBaseClient_WebRestful/webapi/htable/delete/table95/rowkey1/cf/column1";
		this.testDelete(path);;
	}
	
	public DataCellBean getPutDataCellBeanInstance(){
		DataCellBean dataCellBean = new DataCellBean();
		dataCellBean.setTableName("table95");
		dataCellBean.setRowkey("rowkey1");
		dataCellBean.setQualifier("column1");
		dataCellBean.setColumnFamily("cf");
		dataCellBean.setValue("value1");
		return dataCellBean;
	}
	public DataCellBean getDeleteDataCellBeanInstance(){
		DataCellBean dataCellBean = new DataCellBean();
		dataCellBean.setTableName("table95");
		dataCellBean.setRowkey("rowkey1");
		dataCellBean.setQualifier("column1");
		dataCellBean.setColumnFamily("cf");
		return dataCellBean;
	}
	@Override
	protected AppDescriptor configure(){
		return new WebAppDescriptor.Builder().build();
	}
	@Override
	protected TestContainerFactory getTestContainerFactory() {
	    return new GrizzlyWebTestContainerFactory();
	}
	private void testDelete(String path){
		WebResource webResource = client().resource(BASEURL);
		WebResource resource = webResource.path(path);
		MessageInfo result = resource.type(MediaType.APPLICATION_JSON).delete(MessageInfo.class);
		assertEquals("success", result.getStatus());
	}
}
