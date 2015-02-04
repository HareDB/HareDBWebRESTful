package com.haredb.client.rest.resource;

import static org.junit.Assert.assertEquals;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.haredb.client.facade.bean.ConnectionBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.util.ConnectionUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import com.sun.jersey.test.framework.spi.container.grizzly2.web.GrizzlyWebTestContainerFactory;
import com.sun.research.ws.wadl.Response;

public class ConnectionResourceTest extends JerseyTest{
	private static final String BASEURL = "http://192.168.1.16:8080";
	
	private static Client client = null;
	private static WebResource webResource;

	@Test
	public void testconnect(){
		WebResource webResource = client().resource(BASEURL);
	
		String path = "/HareDB_HBaseClient_WebRestful/webapi/connect";
		             
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		
		String input = gson.toJson(getConnectionBean());
		System.out.println(input);
		WebResource resource = webResource.path(path);
		resource.type(MediaType.APPLICATION_JSON).post(input);
	}

	
	private ConnectionBean getConnectionBean(){
		ConnectionBean connection = new ConnectionBean();
		connection.setZookeeperHost("host1");
		connection.setZookeeperPort("2181");
		connection.setNameNodeHostPort("hdfs://host1:9000");
		connection.setRmAddressHostPort("host1:8032");
		connection.setRmSchedulerAddressHostPort("host1:8030");
		connection.setRmResourceTrackerAddressHostPort("host1:8031");
		connection.setRmAdminAddressHostPort("host1:8033");
		connection.setMrJobhistoryAddress("host1:10020");
		connection.setYarnNodeManagerAuxServices("mapreduce_shuffle");
		
//		connection.setHiveConnType(EnumHiveMetaStoreConnectType.EMBED);
		return connection;
	}
	
	@Override
	protected AppDescriptor configure(){
		return new WebAppDescriptor.Builder().build();
	}
	@Override
	protected TestContainerFactory getTestContainerFactory() {
	    return new GrizzlyWebTestContainerFactory();
	}
}
