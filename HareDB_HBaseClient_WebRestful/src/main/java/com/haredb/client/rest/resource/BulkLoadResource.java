package com.haredb.client.rest.resource;

import java.io.File;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.haredb.client.facade.bean.BulkloadStatusBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.UploadSchemaBean;
import com.haredb.client.facade.operator.HareBulkLoadDataBySchema;
import com.haredb.client.facade.operator.HareBulkLoadInQueueOperator;
import com.haredb.client.facade.operator.HareBulkLoadStatus;
import com.haredb.client.util.ConnectionUtil;

@Path("bulkload")
public class BulkLoadResource {

	
	private static Logger logger = Logger.getLogger(IndexResource.class);
	
	@POST
	@Path("schema/upload")
	@Produces(MediaType.APPLICATION_JSON)
	public BulkloadStatusBean executeBulkload(@Context HttpServletRequest request, UploadSchemaBean uplodSchemaBean){
		logger.info("start bulkload");
		BulkloadStatusBean bReturn = null;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareBulkLoadDataBySchema bulkloadData = new HareBulkLoadDataBySchema(connectionUtil.getConnection(request),uplodSchemaBean);
			bulkloadData.setContainerRealPath((new File(request.getRealPath("/")).getPath()));
			bReturn=bulkloadData.runSchemaBulkload();
			logger.info("bulkload finish");
		} catch (Exception e) {
			bReturn = new BulkloadStatusBean();
			bReturn.setStatus(MessageInfo.ERROR);
			bReturn.setException(e.getMessage());
			logger.info("bulkload failed");
			logger.error(e.getMessage());
		}
		return bReturn;
	}
	
	@POST
	@Path("status")
	@Produces(MediaType.APPLICATION_JSON)
	public BulkloadStatusBean bulkloadStatus(@Context HttpServletRequest request, BulkloadStatusBean statusBean){
		logger.info("start get bulkload status");
		BulkloadStatusBean bReturn = null;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareBulkLoadStatus bulkloadStatus = new HareBulkLoadStatus(connectionUtil.getConnection(request));
			bReturn=bulkloadStatus.getBulkLoadStatus(statusBean.getJobName());
			logger.info("get bulkload status finish");
		} catch (Exception e) {
			bReturn = new BulkloadStatusBean();
			bReturn.setStatus(MessageInfo.ERROR);
			bReturn.setException(e.getMessage());
			logger.info("get bulkload status failed");
			logger.error(e.getMessage());
		}
		return bReturn;
	}
	
	@POST
	@Path("schema/scheduledupload")
	@Produces(MediaType.APPLICATION_JSON)
	public BulkloadStatusBean executeBulkload2(@Context HttpServletRequest request, UploadSchemaBean uplodSchemaBean){
		logger.info("start bulkload within queue");
		BulkloadStatusBean bReturn = null;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareBulkLoadInQueueOperator bulkloadData = new HareBulkLoadInQueueOperator(connectionUtil.getConnection(request), uplodSchemaBean);
			bulkloadData.setContainerRealPath(new File(request.getRealPath("/")).getPath());
			bReturn = bulkloadData.runBulkloadInQueue();
			logger.info("bulkload within queue finish, please trace queue status");
		} catch (Exception e) {
			bReturn = new BulkloadStatusBean();
			bReturn.setStatus(MessageInfo.ERROR);
			bReturn.setException(e.getMessage());
			logger.info("bulkload within queue failed");
			logger.error(e.getMessage());
		}
		return bReturn;
	}

	
	
	
}
