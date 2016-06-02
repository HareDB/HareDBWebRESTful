package com.haredb.client.rest.resource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.QueueBean;
import com.haredb.client.facade.bean.QueueStatusBean;
import com.haredb.client.facade.operator.HareQueueOperator;
import com.haredb.client.util.ConnectionUtil;




@Path("jobqueue")
public class JobQueueResource {
	
	@POST
	@Path("schema/status")
	@Produces(MediaType.APPLICATION_JSON)
	public QueueStatusBean getStatus(@Context HttpServletRequest request,QueueBean qBean){
		QueueStatusBean qReturn = null;
		ConnectionUtil connectionUtil = new ConnectionUtil();
		HareQueueOperator operator = new HareQueueOperator(connectionUtil.getConnection(request), qBean);
		try {
			qReturn = operator.getStatus();
		} catch (Exception e) {
			qReturn = new QueueStatusBean();
			qReturn.setStatus(MessageInfo.ERROR);
			qReturn.setException(e.getMessage());
		}
		return qReturn;
	}
	
	
	@POST
	@Path("schema/restart")
	@Produces(MediaType.APPLICATION_JSON)
	public QueueStatusBean restartQueue(@Context HttpServletRequest request,QueueBean qBean){
		QueueStatusBean qReturn = null;
		ConnectionUtil connectionUtil = new ConnectionUtil();
		HareQueueOperator operator = new HareQueueOperator(connectionUtil.getConnection(request), qBean);
		try {
			qReturn = operator.restartQueue();
		} catch (Exception e) {
			qReturn = new QueueStatusBean();
			qReturn.setStatus(MessageInfo.ERROR);
			qReturn.setException(e.getMessage());
		}
		return qReturn;
		
	}
	
	
	@POST
	@Path("schema/dropQueue")
	@Produces(MediaType.APPLICATION_JSON)
	public QueueStatusBean dropQueue(@Context HttpServletRequest request,QueueBean qBean){
		QueueStatusBean qReturn = null;
		ConnectionUtil connectionUtil = new ConnectionUtil();
		HareQueueOperator operator = new HareQueueOperator(connectionUtil.getConnection(request), qBean);
		try {
			qReturn = operator.dropQueue();
		} catch (Exception e) {
			qReturn = new QueueStatusBean();
			qReturn.setStatus(MessageInfo.ERROR);
			qReturn.setException(e.getMessage());
		}
		return qReturn;
	}
	
	
	@POST
	@Path("schema/deleteJob")
	@Produces(MediaType.APPLICATION_JSON)
	public QueueStatusBean deleteJob(@Context HttpServletRequest request,QueueBean qBean){
		QueueStatusBean qReturn = null;
		ConnectionUtil connectionUtil = new ConnectionUtil();
		HareQueueOperator operator = new HareQueueOperator(connectionUtil.getConnection(request), qBean);
		try {
			qReturn = operator.deleteQueueJob();
		} catch (Exception e) {
			qReturn = new QueueStatusBean();
			qReturn.setStatus(MessageInfo.ERROR);
			qReturn.setException(e.getMessage());
		}
		return qReturn;
	}
	
	@POST
	@Path("schema/getQueueFile")
	@Produces(MediaType.APPLICATION_JSON)
	public QueueStatusBean getQueueFile(@Context HttpServletRequest request,QueueBean qBean){
		QueueStatusBean qReturn = null;
		ConnectionUtil connectionUtil = new ConnectionUtil();
		HareQueueOperator operator = new HareQueueOperator(connectionUtil.getConnection(request), qBean);
		try {
			qReturn = operator.getQueueJobFiles();
		} catch (Exception e) {
			qReturn = new QueueStatusBean();
			qReturn.setStatus(MessageInfo.ERROR);
			qReturn.setException(e.getMessage());
		}
		return qReturn;
	}
	
	@POST
	@Path("schema/forceComplete")
	@Produces(MediaType.APPLICATION_JSON)
	public QueueStatusBean forceComplete(@Context HttpServletRequest request, QueueBean qBean){
		QueueStatusBean qReturn = null;
		ConnectionUtil connectionUtil = new ConnectionUtil();
		HareQueueOperator operator = new HareQueueOperator(connectionUtil.getConnection(request), qBean);
		try {
			qReturn = operator.forceChangeToComplete();
		} catch (Exception e) {
			qReturn = new QueueStatusBean();
			qReturn.setStatus(MessageInfo.ERROR);
			qReturn.setException(e.getMessage());
		}
		return qReturn;
	}

	@POST
	@Path("schema/runningJobName")
	@Produces(MediaType.APPLICATION_JSON)
	public QueueStatusBean runningJobName(@Context HttpServletRequest request, QueueBean qBean){
		QueueStatusBean qReturn = null;
		ConnectionUtil connectionUtil = new ConnectionUtil();
		HareQueueOperator operator = new HareQueueOperator(connectionUtil.getConnection(request), qBean);
		try {
			qReturn = operator.runningJobName();
		} catch (Exception e) {
			qReturn = new QueueStatusBean();
			qReturn.setStatus(MessageInfo.ERROR);
			qReturn.setException(e.getMessage());
		}
		return qReturn;
	}
}
