package com.haredb.client.facade.operator;

import com.haredb.client.facade.bean.BulkloadStatusBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.hbase.bulkload.BulkloadObserver;
import com.haredb.hbase.bulkload.bean.BulkloadStatus;
import com.haredb.hbaseclient.core.Connection;

public class HareBulkLoadStatus extends HareContrivance{

	private BulkloadStatusBean statusBean = null;
	private BulkloadStatus serverStatus = null;
	public HareBulkLoadStatus(Connection connection){
		super(connection);
	}
	
	public BulkloadStatusBean getBulkLoadStatus(String jobName) {
		statusBean = new BulkloadStatusBean();
		if(jobName == null || jobName == "") {
			statusBean.setStatus(MessageInfo.ERROR);
			statusBean.setException("Job name is null!");
			return statusBean;
		}
		
		BulkloadObserver obServer = new BulkloadObserver(this.connection);
		try {
			this.statusBean.setJobName(jobName);
			long startTime = System.currentTimeMillis();
			this.serverStatus = obServer.getJobStatus(jobName);
			long endTime = System.currentTimeMillis();
			this.statusBean.setResponseTime(endTime - startTime);
			this.transObject();
		} catch (Exception e) {
			this.statusBean.setStatus(MessageInfo.ERROR);
			this.statusBean.setException(printStackTrace(e));
			return statusBean;
		}
		return statusBean;
	}
	
	/**
	 * transform job status object to Facade object
	 */
	protected void transObject() {
		if(this.serverStatus.getJobId() == null) {
			this.statusBean.setStatus(MessageInfo.ERROR);
			this.statusBean.setException("No such job running on server,please confirm job name!");
			return;
		} else {
			this.statusBean.setStatus(MessageInfo.SUCCESS);
			this.statusBean.setJobId(this.serverStatus.getJobId());
		}
		this.statusBean.setJobStatus(this.serverStatus.getJobStatus());
		this.statusBean.setMapProgress(this.serverStatus.getMapProgress());
		this.statusBean.setReduceProgress(this.serverStatus.getReduceProgress());
//		this.statusBean.setTrackerLink(this.serverStatus.getTrackerLink());
		this.statusBean.setJobErrMessage(this.serverStatus.getJobErrMessage());
		this.statusBean.setBulkloadStartTime(this.serverStatus.getStartTime());
		this.statusBean.setBulkloadFinishTime(this.serverStatus.getFinishTime());
	}
}
