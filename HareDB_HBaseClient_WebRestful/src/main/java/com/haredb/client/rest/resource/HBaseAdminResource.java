package com.haredb.client.rest.resource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.MetaColumnFamilyBean;
import com.haredb.client.facade.bean.TableInfoBean;
import com.haredb.client.facade.operator.HareDefineHTable;
import com.haredb.client.util.ConnectionUtil;
import com.haredb.hbaseclient.core.Connection;

@Path("hbaseadmin")
public class HBaseAdminResource {
	public HBaseAdminResource(){
		
	}

	@POST
	@Path("create")
	@Produces(MediaType.APPLICATION_JSON)
	public MessageInfo createHTable(@Context HttpServletRequest request, TableInfoBean tableInfoBean){
		MessageInfo mReturn = null;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareDefineHTable defineHTable = new HareDefineHTable(connectionUtil.getConnection(request));
			mReturn=defineHTable.createHTable(tableInfoBean);
		} catch (Exception e) {
			mReturn = new MessageInfo();
			mReturn.setStatus(MessageInfo.ERROR);
			mReturn.setException(e.getMessage());
		}
		return mReturn;
	}
	
	@POST
	@Path("drop")
	@Produces(MediaType.APPLICATION_JSON)
	public MessageInfo dropHTable(@Context HttpServletRequest request, TableInfoBean tableInfoBean){
		MessageInfo mReturn = null;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareDefineHTable defineHTable = new HareDefineHTable(connectionUtil.getConnection(request));
			mReturn = defineHTable.dropHTable(tableInfoBean.getTableName());
		} catch (Exception e) {
			mReturn = new MessageInfo();
			mReturn.setStatus(MessageInfo.ERROR);
			mReturn.setException(e.getMessage());
		}
		return mReturn;
	}
	
	
	@PUT
	@Path("alter/{tableName}")
	@Produces(MediaType.APPLICATION_JSON)
	public MessageInfo alterHTableMeta(@Context HttpServletRequest request, @PathParam("tableName") String tableName, MetaColumnFamilyBean columnFamilyBean){
		MessageInfo mReturn = null;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareDefineHTable defineHTable = new HareDefineHTable(connectionUtil.getConnection(request));
			mReturn=defineHTable.alterHTable(tableName, columnFamilyBean);
		} catch (Exception e) {
			mReturn = new MessageInfo();
			mReturn.setStatus(MessageInfo.ERROR);
			mReturn.setException(e.getMessage());
		}
		return mReturn;
	}
}
