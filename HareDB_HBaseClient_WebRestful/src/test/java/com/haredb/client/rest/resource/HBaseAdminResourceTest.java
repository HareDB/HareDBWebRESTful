package com.haredb.client.rest.resource;


import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.MetaColumnFamilyBean;
import com.haredb.client.facade.bean.TableInfoBean;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import com.sun.jersey.test.framework.spi.container.grizzly2.web.GrizzlyWebTestContainerFactory;
import com.sun.net.httpserver.HttpServer;

public class HBaseAdminResourceTest extends JerseyTest{
	public static final String BASEURL = "http://211.79.198.132:8080/HareRestful";
	private HttpServer server;
	
	
	@Test
	public void testcreateHTable(){
		WebResource webResource = client().resource(BASEURL);
		String path = "/webapi/hbaseadmin/create";
		             
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		
		
		String input = gson.toJson(getCreateMetaColumnFamilyBeanInstances());
		
		WebResource resource = webResource.path(path);
		MessageInfo result = resource.type(MediaType.APPLICATION_JSON).post(MessageInfo.class, input);
		assertEquals("success", result.getStatus());
	}
	
	
	@Test
	public void testalterHTable(){
		WebResource webResource = client().resource(BASEURL);
		String path = "/webapi/hbaseadmin/alter/table1";
		
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		
		String input = gson.toJson(getAlterMetaColumnFamilyBeanInstance());
			
		WebResource resource = webResource.path(path);
		MessageInfo result = resource.type(MediaType.APPLICATION_JSON).put(MessageInfo.class, input);
		assertEquals("success", result.getStatus());
	}
	
	@Test
	public void testdropHTable(){
		WebResource webResource = client().resource(BASEURL);
		String path = "/webapi/hbaseadmin/drop/table1";
		
		WebResource resource = webResource.path(path);
		MessageInfo result = resource.type(MediaType.APPLICATION_JSON).delete(MessageInfo.class);
		assertEquals("success", result.getStatus());
		System.out.println(result.getResponseTime());
	}
	
	private MetaColumnFamilyBean getAlterMetaColumnFamilyBeanInstance(){
		MetaColumnFamilyBean cf = new MetaColumnFamilyBean();
		cf.setColumnFamilyName("cf");
		cf.setTtl(654321);
		return cf;
	}
	
	
	private TableInfoBean getCreateMetaColumnFamilyBeanInstances(){
		TableInfoBean tableInfoBean = new TableInfoBean();
		tableInfoBean.setTableName("table1");
		
		List<String> columnFamilys = new ArrayList<String>();
		columnFamilys.add("cf");
		tableInfoBean.setColumnFamilys(columnFamilys);
		
		return tableInfoBean;
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
