package com.haredb.client.rest.resource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.haredb.client.facade.bean.DataCellBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.ScanConditionBean;
import com.haredb.client.facade.bean.ScanResultStatusBean;
import com.haredb.client.facade.operator.HareManipulationHTable;
import com.haredb.client.facade.operator.HareQueryScan;
import com.haredb.client.util.ConnectionUtil;
import com.haredb.hbaseclient.core.Connection;


@Path("htable")
public class HTableResource {
	public HTableResource(){

	}
	
	

	@POST
	@Path("scan")
	@Produces(MediaType.APPLICATION_JSON)
	public MessageInfo scanHTable(@Context HttpServletRequest request, ScanConditionBean bean){
		MessageInfo mReturn;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareQueryScan queryScan = new HareQueryScan(connectionUtil.getConnection(request));
			mReturn = queryScan.scanHTable(bean.getTableName(), bean.getPageSize(), bean.getLimit());
		}catch(Exception e) {
			mReturn = new ScanResultStatusBean();
			mReturn.setStatus(MessageInfo.ERROR);
			mReturn.setException(e.getMessage());
		}
		return mReturn;
	}

	@POST
	@Path("put")
	@Produces(MediaType.APPLICATION_JSON)
	public MessageInfo htablePut(@Context HttpServletRequest request, DataCellBean dataCellBean){
		MessageInfo mReturn;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareManipulationHTable putData = new HareManipulationHTable(connectionUtil.getConnection(request));
			mReturn=putData.htablePut(dataCellBean);
		}catch(Exception e) {
			mReturn = new MessageInfo();
			mReturn.setStatus(MessageInfo.ERROR);
			mReturn.setException(e.getMessage());
		}
		return mReturn;
	}
	
	@DELETE
	@Path("delete/{tableName}/{rowkey}/{columnfamily}/{qualifier}")
	@Produces(MediaType.APPLICATION_JSON)
	public MessageInfo htableDelete(@Context HttpServletRequest request, @PathParam("tableName") String tableName, @PathParam("rowkey") String rowkey, 
			                @PathParam("columnfamily") String columnFamily, @PathParam("qualifier") String qualifier){
		MessageInfo mReturn;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareManipulationHTable deleteData = new HareManipulationHTable(connectionUtil.getConnection(request));
			mReturn = deleteData.htableDelete(tableName, rowkey, columnFamily, qualifier);
		}catch(Exception e) {
			mReturn = new MessageInfo();
			mReturn.setStatus(MessageInfo.ERROR);
			mReturn.setException(e.getMessage());
		}
		
		
		return mReturn;
	}
	
	@DELETE
	@Path("delete/{tableName}/{rowkey}/{columnfamily}")
	@Produces(MediaType.APPLICATION_JSON)
	public MessageInfo htableDelete(@Context HttpServletRequest request, @PathParam("tableName") String tableName, @PathParam("rowkey") String rowkey, 
			                @PathParam("columnfamily") String columnFamily){
		MessageInfo mReturn;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareManipulationHTable deleteData = new HareManipulationHTable(connectionUtil.getConnection(request));
			mReturn =deleteData.htableDelete(tableName, rowkey, columnFamily);
		}catch(Exception e) {
			mReturn = new MessageInfo();
			mReturn.setStatus(MessageInfo.ERROR);
			mReturn.setException(e.getMessage());
		}
		
		
		return mReturn;
	}
	
	@DELETE
	@Path("delete/{tableName}/{rowkey}")
	@Produces(MediaType.APPLICATION_JSON)
	public MessageInfo htableDelete(@Context HttpServletRequest request, @PathParam("tableName") String tableName, @PathParam("rowkey") String rowkey){
		MessageInfo mReturn;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareManipulationHTable deleteData = new HareManipulationHTable(connectionUtil.getConnection(request));
			mReturn = deleteData.htableDelete(tableName, rowkey);
		}catch(Exception e) {
			mReturn = new MessageInfo();
			mReturn.setStatus(MessageInfo.ERROR);
			mReturn.setException(e.getMessage());
		}
		
		
		return mReturn;
	}
}