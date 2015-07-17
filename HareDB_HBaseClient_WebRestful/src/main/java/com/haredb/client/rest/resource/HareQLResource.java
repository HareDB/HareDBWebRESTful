package com.haredb.client.rest.resource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.haredb.client.facade.bean.HareQLQueryBean;
import com.haredb.client.facade.bean.HareQLResultStatusBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.operator.HareQueryHareQL;
import com.haredb.client.util.ConnectionUtil;


@Path("hareql")
public class HareQLResource {

	public HareQLResource(){

	}
	
	@POST
	@Path("query")
	@Produces(MediaType.APPLICATION_JSON)
	public HareQLResultStatusBean executeHareQL(@Context HttpServletRequest request, HareQLQueryBean queryBean){
		HareQLResultStatusBean hrsBean;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareQueryHareQL hareQL = new HareQueryHareQL(connectionUtil.getConnection(request), connectionUtil.getHiveMetaDataConnection(request));
			hrsBean =hareQL.executeHareQL(queryBean.getTempFilePath(), queryBean.getSql(), queryBean.getPage(), queryBean.getLimit()); 
		} catch (Exception e) {
			hrsBean = new HareQLResultStatusBean();
			hrsBean.setStatus(MessageInfo.ERROR);
			hrsBean.setException(e.getMessage());
		}
		
		return hrsBean;
	}
	
	@POST
	@Path("submit")
	@Produces(MediaType.APPLICATION_JSON)
	public HareQLResultStatusBean submitHareQL(@Context HttpServletRequest request, HareQLQueryBean queryBean) {
		HareQLResultStatusBean hrsBean;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareQueryHareQL hareQL = new HareQueryHareQL(connectionUtil.getConnection(request), connectionUtil.getHiveMetaDataConnection(request));
			hrsBean =hareQL.submitHareQL(queryBean.getTempFilePath(), queryBean.getSql(), queryBean.getPage(), queryBean.getLimit()); 
		} catch (Exception e) {
			hrsBean = new HareQLResultStatusBean();
			hrsBean.setStatus(MessageInfo.ERROR);
			hrsBean.setException(e.getMessage());
		}
		return hrsBean;
	}
	
	@POST
	@Path("status")
	@Produces(MediaType.APPLICATION_JSON)
	public HareQLResultStatusBean hareQueryStatus(@Context HttpServletRequest request, HareQLQueryBean queryBean){
		HareQLResultStatusBean hrsBean;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareQueryHareQL hareQL = new HareQueryHareQL(connectionUtil.getConnection(request), connectionUtil.getHiveMetaDataConnection(request));
			hrsBean =hareQL.getHareQLStatus(queryBean.getTempFilePath()); 
		} catch (Exception e) {
			hrsBean = new HareQLResultStatusBean();
			hrsBean.setStatus(MessageInfo.ERROR);
			hrsBean.setException(e.getMessage());
		}
		return hrsBean;
	}
	
}
