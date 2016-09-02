package com.haredb.client.facade.operator;

import org.junit.Test;

import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.ScanResultStatusBean;
import com.haredb.hbaseclient.core.Connection;

public class HareQueryScanTest {

	@Test
	public void testScan(){
		Connection connection = new Connection("acer-edc1", "2181");
		connection.create();
		HareQueryScan queryScan = new HareQueryScan(connection);
		ScanResultStatusBean lists = (ScanResultStatusBean)queryScan.scanHTable("table2", 1, 1);
		System.out.println(lists.getHeads());
		System.out.println(lists.getResults());
		String results[][] = lists.getResults();
		
		for(int i = 0 ; i < results.length ; i++){
			for(int j = 0 ; j < lists.getHeads().size(); j++){
				System.out.print(results[i][j] + "    ");
			}
			System.out.println("");
		}
	}
}
