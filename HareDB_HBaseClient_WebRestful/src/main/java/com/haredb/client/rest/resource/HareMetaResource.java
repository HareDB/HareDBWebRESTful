package com.haredb.client.rest.resource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.MetaTableBean;
import com.haredb.client.facade.operator.HareMetaStoreOperator;
import com.haredb.client.util.ConnectionUtil;

@Path("haremeta")
public class HareMetaResource {
	
	@POST
	@Path("create")
	@Produces(MediaType.APPLICATION_JSON)
	public MessageInfo createMetaTable(@Context HttpServletRequest request, MetaTableBean metaBean){
		MessageInfo mReturn = null;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareMetaStoreOperator metaOperator = new HareMetaStoreOperator(connectionUtil.getHiveMetaDataConnection(request));
			mReturn = metaOperator.createTable(metaBean);
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
	public MessageInfo dropMetaTable(@Context HttpServletRequest request, MetaTableBean metaBean){
		MessageInfo mReturn = null;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareMetaStoreOperator metaOperator = new HareMetaStoreOperator(connectionUtil.getHiveMetaDataConnection(request));
			mReturn = metaOperator.dropTable(metaBean.getMetaTableName());
		} catch (Exception e) {
			mReturn = new MessageInfo();
			mReturn.setStatus(MessageInfo.ERROR);
			mReturn.setException(e.getMessage());
		}
		return mReturn;
	}
	
	@POST
	@Path("alter")
	@Produces(MediaType.APPLICATION_JSON)
	public MessageInfo alterMetaTable(@Context HttpServletRequest request, MetaTableBean metaBean){
		MessageInfo mReturn = null;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareMetaStoreOperator metaOperator = new HareMetaStoreOperator(connectionUtil.getHiveMetaDataConnection(request));
			mReturn = metaOperator.alterTable(metaBean);
		} catch (Exception e) {
			mReturn = new MessageInfo();
			mReturn.setStatus(MessageInfo.ERROR);
			mReturn.setException(e.getMessage());
		}
		return mReturn;
	}
	
	@POST
	@Path("describe")
	@Produces(MediaType.APPLICATION_JSON)
	public MetaTableBean describeMetaTable(@Context HttpServletRequest request, MetaTableBean metaBean){
		MetaTableBean mReturn = null;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareMetaStoreOperator metaOperator = new HareMetaStoreOperator(connectionUtil.getHiveMetaDataConnection(request));
			mReturn = metaOperator.describeTable(metaBean.getMetaTableName());
		} catch (Exception e) {
			mReturn = new MetaTableBean();
			mReturn.setStatus(MessageInfo.ERROR);
			mReturn.setException(e.getMessage());
		}
		return mReturn;
	}
	
}
