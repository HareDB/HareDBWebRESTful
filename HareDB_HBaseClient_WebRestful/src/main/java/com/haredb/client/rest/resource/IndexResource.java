package com.haredb.client.rest.resource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.haredb.client.facade.bean.IndexBean;
import com.haredb.client.facade.bean.IndexStatusBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.operator.HareIndexOperator;
import com.haredb.client.util.ConnectionUtil;


@Path("index")
public class IndexResource {
	
	private static Logger logger = Logger.getLogger(IndexResource.class);
	
	@POST
	@Path("schema/createindex")
	@Produces(MediaType.APPLICATION_JSON)
	public IndexStatusBean createIndex(@Context HttpServletRequest request, IndexBean indexBean){
		logger.info("start create Index");
		IndexStatusBean iReturn = null;
		ConnectionUtil connectionUtil = new ConnectionUtil();
		HareIndexOperator operator = new HareIndexOperator(connectionUtil.getConnection(request), indexBean);
		
		try {
			iReturn = operator.createIndex();
			logger.info("create Index finish.");
		} catch (Exception e) {
			iReturn = new IndexStatusBean();
			iReturn.setStatus(MessageInfo.ERROR);
			iReturn.setException(e.getMessage());
			logger.info("create Index failed.");
		}
		return iReturn;
	}
	
	
	@POST
	@Path("schema/updateindex")
	@Produces(MediaType.APPLICATION_JSON)
	public IndexStatusBean updateIndex(@Context HttpServletRequest request, IndexBean indexBean){
		logger.info("start update Index");
		IndexStatusBean iReturn = null;
		ConnectionUtil connectionUtil = new ConnectionUtil();
		HareIndexOperator operator = new HareIndexOperator(connectionUtil.getConnection(request), indexBean);
		
		try {
			iReturn = operator.updateIndex();
			logger.info("update Index finish.");
		} catch (Exception e) {
			iReturn = new IndexStatusBean();
			iReturn.setStatus(MessageInfo.ERROR);
			iReturn.setException(e.getMessage());
			logger.info("update Index failed.");
		}
		return iReturn;
	}
	
	
	@POST
	@Path("schema/drop")
	@Produces(MediaType.APPLICATION_JSON)
	public IndexStatusBean dropIndex(@Context HttpServletRequest request, IndexBean indexBean){
		logger.info("start drop Index");
		IndexStatusBean iReturn = null;
		ConnectionUtil connectionUtil = new ConnectionUtil();
		HareIndexOperator operator = new HareIndexOperator(connectionUtil.getConnection(request), indexBean);
		try {
			iReturn = operator.dropIndex();
			logger.info("drop Index finish");
		} catch (Exception e) {
			iReturn = new IndexStatusBean();
			iReturn.setStatus(MessageInfo.ERROR);
			iReturn.setException(e.getMessage());
			logger.info("drop Index failed");
		}
		return iReturn;
	}
}
