package com.haredb.client.rest.resource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.ClientContextConfigurer;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.junit.Test;

public class TestNewConnection {
private static CookieStore cookieStore;
	
	@Test 
	public void test() throws ClientProtocolException, IOException{
		
		try {

			DefaultHttpClient httpClient = new DefaultHttpClient();
//			HttpPost httpPost = new HttpPost("http://10.28.5.1:8080/HareDBClient/webapi/connect");
			HttpPost httpPost = new HttpPost("http://211.79.205.17:8080/HareDBClient/webapi/connect");
			
			String s = "{\"zookeeperHost\":\"edc-sl-234\",\"zookeeperPort\":\"2181\",\"nameNodeHostPort\":\"hdfs://edc-sl-234:8020\",\"rmAddressHostPort\":\"edc-sl-234:8032\",\"rmSchedulerAddressHostPort\":\"edc-sl-234:8030\",\"rmResourceTrackerAddressHostPort\":\"edc-sl-234:8031\",\"rmAdminAddressHostPort\":\"edc-sl-234:8033\",\"mrJobhistoryAddress\":\"edc-sl-234:10020\",\"hiveConnType\":\"SERVER2\",\"metaStoreConnectURL\":\"jdbc:mysql:// 10.28.5.1:3306/hare\",\"metaStoreConnectDriver\":\"com.mysql.jdbc.Driver\",\"metaStoreConnectUserName\":\"root\",\"metaStoreConnectPassword\":\"123456\",\"solrZKHosts\":\"edc-sl-234:9983\",\"dbName\":\"default\",\"dbBrand\":\"MYSQL\",\"enableKerberos\":\"false\",\"account\":\"!@#!$%$^\",\"pwd\":\"^$%&%(^*\"}";
			StringEntity input = new StringEntity(s);
			HttpEntity httpinput = new ByteArrayEntity(s.getBytes());
			
			input.setContentType("application/json");
			httpPost.setEntity(input);

			HttpResponse response = httpClient.execute(httpPost);

			if (response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "	+ response.getStatusLine().getStatusCode());
			}

			BufferedReader br = new BufferedReader( new InputStreamReader((response.getEntity().getContent())));

			String output;
			System.out.println("Output from Server .... \n");
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}

			System.out.println("Initial set of cookies:");
			
			/* show all cookie*/
			cookieStore = httpClient.getCookieStore();
	        List<org.apache.http.cookie.Cookie> cookies = cookieStore.getCookies();
	        if (cookies.isEmpty()) {
	            System.out.println("None");
	        } else {
	            for (int i = 0; i < cookies.size(); i++) {
	                System.out.println("- " + cookies.get(i).toString());
	            }
	        }
			
			httpClient.getConnectionManager().shutdown();

		  } catch (MalformedURLException e) {
			  e.printStackTrace();
		  } catch (IOException e) {
			  e.printStackTrace();
		  }
		
		
		test2();

	}
	
	
	
	
	public void test2() throws ClientProtocolException, IOException{
		DefaultHttpClient httpClient = new DefaultHttpClient();
		HttpPost httpPost = new HttpPost("http://211.79.205.17:8080/HareDBClient/webapi/jobqueue/schema/status");
		String s = "{\"tableName\":\"frank_test2\",\"resultPath\":\"hdfs://edc-sl-234:8020/tmp/frank_test_result.txt\"}";
		StringEntity input = new StringEntity(s);
		input.setContentType("application/json");
		httpPost.setEntity(input);
		
		HttpContext localContext = new BasicHttpContext();
		localContext.setAttribute(ClientContextConfigurer.COOKIE_STORE, this.cookieStore);
		
		
		HttpResponse response = httpClient.execute(httpPost,localContext);
		
		
		
		
		BufferedReader br = new BufferedReader( new InputStreamReader((response.getEntity().getContent())));

		String output;
		System.out.println("Output from Server .... \n");
		while ((output = br.readLine()) != null) {
			System.out.println(output);
		}
		
	}
}
