package com.haredb.client.rest.resource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.haredb.client.facade.bean.ConnectionBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.util.ConnectionUtil;

@Path("connect")
public class ConnectionResource {

	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public MessageInfo connect(@Context HttpServletRequest request, ConnectionBean connectionBean){
		
		ConnectionUtil connUtil = new ConnectionUtil(connectionBean);
		MessageInfo info = connUtil.resolveBean();
		if(info.getStatus().equals(MessageInfo.SUCCESS)) {
			request.getSession().setAttribute(ConnectionUtil.connectionSessionName, connUtil.getConn());
			request.getSession().setAttribute(ConnectionUtil.hiveMetaDataSessionName, connUtil.getMetaConn());
		}
		return info;
	}
	
}
