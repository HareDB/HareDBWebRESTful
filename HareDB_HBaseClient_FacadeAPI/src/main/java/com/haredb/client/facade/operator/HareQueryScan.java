package com.haredb.client.facade.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.util.Bytes;
import com.haredb.ResolveSymbolConstant;
import com.haredb.client.facade.bean.DataCellBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.ScanResultStatusBean;
import com.haredb.hbaseclient.core.Connection;
import com.haredb.hbaseclient.core.TableObject;
import com.haredb.hbaseclient.core.data.QueryData;

public class HareQueryScan extends HareContrivance{
	
	public HareQueryScan(Connection connection){
		super(connection);
	}
	
	public ScanResultStatusBean scanHTable(String tableName, int pageSize, int limit){
		ScanResultStatusBean resultStatusBean = new ScanResultStatusBean();
		try{
			long startTime = System.currentTimeMillis();
			
			QueryData qd = new QueryData(this.connection);
			TableObject coreData = qd.queryAllData(tableName, Integer.valueOf(limit), Integer.valueOf(pageSize));
			List<DataCellBean> cellHeads = new ArrayList<DataCellBean>();
			
			DataCellBean cellBean;
			String[] cfQualifier;
			//heads handler
			for(String qualifier:coreData.getQualifierList()) {
				cellBean = new DataCellBean();
				if(qualifier.equals(":key")) {
					cellBean.setRowkey(qualifier);
				} else {
					cfQualifier=qualifier.split(ResolveSymbolConstant.KW_Colon);
					cellBean.setColumnFamily(cfQualifier[0]);
					cellBean.setQualifier(cfQualifier[1]);
				}
				cellHeads.add(cellBean);
			}
			
			//data handler
			Map<String, byte[]> rowData;
			String value;
			String results[][] = new String[coreData.getDataMap().size()][cellHeads.size()];
			int colIndex=0;
			for(int i=0; i<coreData.getDataMap().size(); i++){
				colIndex=0;
				rowData=coreData.getDataMap().get(i);
				for(String key : coreData.getQualifierList()){
					if(rowData.get(key) != null) {
						value = Bytes.toString(rowData.get(key));
					} else {
						value="";
					}
					results[i][colIndex]=value;
					colIndex++;
				}
			}
			
			long endTime = System.currentTimeMillis();
			resultStatusBean.setResponseTime(endTime - startTime);
			resultStatusBean.setHeads(cellHeads);
			resultStatusBean.setResults(results);
			resultStatusBean.setStatus(MessageInfo.SUCCESS);
		}catch(Exception e){
			resultStatusBean.setStatus(MessageInfo.ERROR);
			resultStatusBean.setException(printStackTrace(e));
			return resultStatusBean;
		}
		return resultStatusBean;
		
	}
}
