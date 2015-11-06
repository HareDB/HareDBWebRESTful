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



public class JavaRestClientSessionTest {
	
	
	private static CookieStore cookieStore;
	
	@Test 
	public void test() throws ClientProtocolException, IOException{
		
		try {

			DefaultHttpClient httpClient = new DefaultHttpClient();
			HttpPost httpPost = new HttpPost("http://localhost:8080/HareDBClient/webapi/connect");
			
			String s = "{\"zookeeperHost\":\"host1\",\"zookeeperPort\":\"2181\",\"nameNodeHostPort\":\"hdfs://host1:8020\",\"rmAddressHostPort\":\"host1:8032\",\"rmSchedulerAddressHostPort\":\"host1:8030\",\"rmResourceTrackerAddressHostPort\":\"host1:8031\",\"rmAdminAddressHostPort\":\"host1:8033\",\"mrJobhistoryAddress\":\"host1:10020\",\"hiveConnType\":\"SERVER2\",\"hiveServer2Url\":\"jdbc:hive2://server-b1:10000/default;principal=hive/_HOST@ISLAND.COM\",\"metaStoreConnectURL\":\"jdbc:mysql://192.168.1.214:3306/hare\",\"metaStoreConnectDriver\":\"com.mysql.jdbc.Driver\",\"metaStoreConnectUserName\":\"root\",\"metaStoreConnectPassword\":\"123456\",\"solrZKHosts\":\"host1:9983\",\"dbName\":\"default\",\"dbBrand\":\"MYSQL\",\"enableKerberos\":\"false\",\"hbaseMasterPrincipal\":\"hbase/_HOST@ISLAND.COM\",\"hbaseRegionServerPrincipal\":\"hbase/_HOST@ISLAND.COM\",\"dfsNameNodePrincipal\":\"hdfs/_HOST@ISLAND.COM\",\"dfsDataNodePrincipal\":\"hdfs/_HOST@ISLAND.COM\",\"hiveMetaStorePrincipal\":\"hive/_HOST@ISLAND.COM\",\"yarnResourceMgrPrincipal\":\"yarn/_HOST@ISLAND.COM\",\"yarnNodeMgrPrincipal\":\"yarn/_HOST@ISLAND.COM\",\"account\":\"!@#!$%$^\",\"pwd\":\"^$%&%(^*\"}";
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
	        List<org.apache.http.cookie.Cookie> cookies = httpClient.getCookieStore().getCookies();
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
		HttpPost httpPost = new HttpPost("http://localhost:8080/HareDBClient/webapi/jobqueue/schema/status");
		String s = "{\"tableName\":\"frank_test2\",\"resultPath\":\"hdfs://host1:8020/tmp/frank_test_result.txt\"}";
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
