package com.haredb.client.rest.resource;

import java.io.File;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.haredb.client.facade.bean.BulkloadStatusBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.client.facade.bean.UploadSchemaBean;
import com.haredb.client.facade.operator.HareBulkLoadDataBySchema;
import com.haredb.client.facade.operator.HareBulkLoadStatus;
import com.haredb.client.util.ConnectionUtil;

@Path("bulkload")
public class BulkLoadResource {

	@POST
	@Path("schema/upload")
	@Produces(MediaType.APPLICATION_JSON)
	public BulkloadStatusBean executeBulkload(@Context HttpServletRequest request, UploadSchemaBean uplodSchemaBean){
		BulkloadStatusBean bReturn = null;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareBulkLoadDataBySchema bulkloadData = new HareBulkLoadDataBySchema(connectionUtil.getConnection(request),uplodSchemaBean);
			bulkloadData.setContainerRealPath((new File(request.getRealPath("/")).getPath()));
			bReturn=bulkloadData.runSchemaBulkload();
		} catch (Exception e) {
			bReturn = new BulkloadStatusBean();
			bReturn.setStatus(MessageInfo.ERROR);
			bReturn.setException(e.getMessage());
		}
		return bReturn;
	}
	
	@POST
	@Path("status")
	@Produces(MediaType.APPLICATION_JSON)
	public BulkloadStatusBean bulkloadStatus(@Context HttpServletRequest request, BulkloadStatusBean statusBean){
		BulkloadStatusBean bReturn = null;
		try {
			ConnectionUtil connectionUtil = new ConnectionUtil();
			HareBulkLoadStatus bulkloadStatus = new HareBulkLoadStatus(connectionUtil.getConnection(request));
			bReturn=bulkloadStatus.getBulkLoadStatus(statusBean.getJobName());
		} catch (Exception e) {
			bReturn = new BulkloadStatusBean();
			bReturn.setStatus(MessageInfo.ERROR);
			bReturn.setException(e.getMessage());
		}
		return bReturn;
	}
}
