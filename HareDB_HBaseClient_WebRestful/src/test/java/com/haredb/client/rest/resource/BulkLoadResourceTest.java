package com.haredb.client.rest.resource;

import javax.ws.rs.core.MediaType;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.haredb.client.facade.bean.BulkloadStatusBean;
import com.haredb.client.facade.bean.UploadSchemaBean;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import com.sun.jersey.test.framework.spi.container.grizzly2.web.GrizzlyWebTestContainerFactory;

public class BulkLoadResourceTest extends JerseyTest{
	private static final String BASEURL = "http://211.79.198.132:8080/HareRestful";
	
	@Test
	public void testexecuteBulkload(){
		WebResource webResource = client().resource(BASEURL);
		String path = "/webapi/bulkload/schema/upload";
		
		WebResource resource = webResource.path(path);
		
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		
		UploadSchemaBean schemaBean = new UploadSchemaBean();
		schemaBean.setSchemaFilePath("hdfs://KE2000-Hadoop1:8020/schema.prop");
		schemaBean.setDataPath("hdfs://KE2000-Hadoop1:8020/northwind.txt");
		schemaBean.setResultPath("hdfs://KE2000-Hadoop1:8020/result.txt");
		
		String input = gson.toJson(schemaBean);
		String result = resource.type(MediaType.APPLICATION_JSON).post(String.class, input);
		System.out.println(result);
	}

	@Test
	public void testbulkloadStatus(){
		WebResource webResource = client().resource(BASEURL);
		String path = "/webapi/bulkload/status/jobid_0000001";
		
		WebResource resource = webResource.path(path);
		
		BulkloadStatusBean result = resource.type(MediaType.APPLICATION_JSON).post(BulkloadStatusBean.class);
		System.out.println(result.getResponseTime());
//		System.out.println(result.getFilePath());
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
