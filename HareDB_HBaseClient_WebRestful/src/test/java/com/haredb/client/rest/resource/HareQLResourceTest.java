package com.haredb.client.rest.resource;

import javax.ws.rs.core.MediaType;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.haredb.client.facade.bean.HareQLQueryBean;
import com.haredb.client.facade.bean.HareQLResultStatusBean;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import com.sun.jersey.test.framework.spi.container.grizzly2.web.GrizzlyWebTestContainerFactory;

public class HareQLResourceTest extends JerseyTest{
	private static final String BASEURL = "http://192.168.1.22:8080";
	
	@Test 
	public void testHareQLSelectForTempFilePath(){
		WebResource webResource = client().resource(BASEURL);
		String sql = "select * from table81";
		String path = "/HareDB_HBaseClient_WebRestful/webapi/hareql/query";
		WebResource resource = webResource.path(path);
		
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		String input = gson.toJson(this.getHareQLQueryBean1());
		HareQLResultStatusBean result = resource.type(MediaType.APPLICATION_JSON).post(HareQLResultStatusBean.class, input);
		System.out.println(result.getStatus());
		System.out.println(result.getResponseTime());
		System.out.println(result.getHeads());
		System.out.println(result.getRowSize());
		System.out.println(result.getResults());
	}
	
	@Test
	public void testHareQLSelect(){
		WebResource webResource = client().resource(BASEURL);
		String sql = "select * from stana";
		String path = "/HareDB_HBaseClient_WebRestful/webapi/hareql/query";
		WebResource resource = webResource.path(path);

		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		String input = gson.toJson(this.getHareQLQueryBean2(sql));
		
		HareQLResultStatusBean result = resource.type(MediaType.APPLICATION_JSON).post(HareQLResultStatusBean.class, input);
		System.out.println(result.getResults());
		System.out.println(result.getStatus());
		System.out.println(result.getResponseTime());
		System.out.println(result.getHeads());
		System.out.println(result.getRowSize());
	}
	@Test
	public void testHareQLInsert(){
		WebResource webResource = client().resource(BASEURL);
		String path = "/HareDB_HBaseClient_WebRestful/webapi/hareql/query/";
		
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		String input = gson.toJson(this.getHareQLQueryBean3());
		
		WebResource resource = webResource.path(path);
		HareQLResultStatusBean result = resource.type(MediaType.APPLICATION_JSON).post(HareQLResultStatusBean.class, input);
		System.out.println(result.getResults());
	}
	@Test
	public void testHareQLDelete(){
		WebResource webResource = client().resource(BASEURL);
		String path = "/HareDB_HBaseClient_WebRestful/webapi/hareql/query/";
		
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		String input = gson.toJson(this.getHareQLQueryBean4());
		
		WebResource resource = webResource.path(path);
		HareQLResultStatusBean result = resource.type(MediaType.APPLICATION_JSON).post(HareQLResultStatusBean.class, input);
		System.out.println(result.getResults());
	}
	
	private HareQLQueryBean getHareQLQueryBean1(){
		HareQLQueryBean queryBean = new HareQLQueryBean();
		queryBean.setLimit(10);
		queryBean.setPage(1);
		queryBean.setSql("select * from table81");
		queryBean.setTempFilePath("/temp/ccc");
		return queryBean;
	}
	private HareQLQueryBean getHareQLQueryBean2(String sql){
		HareQLQueryBean queryBean = new HareQLQueryBean();
		queryBean.setLimit(10);
		queryBean.setPage(1);
		queryBean.setSql(sql);
		return queryBean;
	}
	private HareQLQueryBean getHareQLQueryBean3(){
		HareQLQueryBean queryBean = new HareQLQueryBean();
		queryBean.setLimit(10);
		queryBean.setPage(1);
		queryBean.setSql("insert into table81(:key, cf:column1) values('rowkey101', 'value100')");
		return queryBean;
	}
	private HareQLQueryBean getHareQLQueryBean4(){
		HareQLQueryBean queryBean = new HareQLQueryBean();
		queryBean.setSql("delete from stana where :key='rk3'");
		return queryBean;
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
