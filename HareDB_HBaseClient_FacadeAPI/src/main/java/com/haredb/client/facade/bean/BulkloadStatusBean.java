package com.haredb.client.facade.bean;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class BulkloadStatusBean extends MessageInfo{
	private String bulkloadStartTime;
	private String bulkloadFinishTime;
	
	private String jobName;
	
	private String jobId;
	private String jobStatus;
	private String trackerLink;
	private String jobErrMessage;
	private String mapProgress;
	private String reduceProgress;
		
	/**
	 * @return the bulkloadStartTime
	 */
	public String getBulkloadStartTime() {
		return bulkloadStartTime;
	}
	/**
	 * @param bulkloadStartTime the bulkloadStartTime to set
	 */
	public void setBulkloadStartTime(String bulkloadStartTime) {
		this.bulkloadStartTime = bulkloadStartTime;
	}
	/**
	 * @return the bulkloadFinishTime
	 */
	public String getBulkloadFinishTime() {
		return bulkloadFinishTime;
	}
	/**
	 * @param bulkloadFinishTime the bulkloadFinishTime to set
	 */
	public void setBulkloadFinishTime(String bulkloadFinishTime) {
		this.bulkloadFinishTime = bulkloadFinishTime;
	}
	
		
	/**
	 * @return the jobName
	 */
	public String getJobName() {
		return jobName;
	}
	/**
	 * @param jobName the jobName to set
	 */
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	
	/**
	 * @return the jobId
	 */
	public String getJobId() {
		return jobId;
	}
	/**
	 * @param jobId the jobId to set
	 */
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	
	/**
	 * @return the jobStatus
	 */
	public String getJobStatus() {
		return jobStatus;
	}
	/**
	 * @param jobStatus the jobStatus to set
	 */
	public void setJobStatus(String jobStatus) {
		this.jobStatus = jobStatus;
	}
	/**
	 * @return the trackerLink
	 */
	public String getTrackerLink() {
		return trackerLink;
	}
	/**
	 * @param trackerLink the trackerLink to set
	 */
	public void setTrackerLink(String trackerLink) {
		this.trackerLink = trackerLink;
	}
	/**
	 * @return the mapProgress
	 */
	public String getMapProgress() {
		return mapProgress;
	}
	/**
	 * @param mapProgress the mapProgress to set
	 */
	public void setMapProgress(String mapProgress) {
		this.mapProgress = mapProgress;
	}
	/**
	 * @return the reduceProgress
	 */
	public String getReduceProgress() {
		return reduceProgress;
	}
	/**
	 * @param reduceProgress the reduceProgress to set
	 */
	public void setReduceProgress(String reduceProgress) {
		this.reduceProgress = reduceProgress;
	}
	
	/**
	 * @return the jobErrMessage
	 */
	public String getJobErrMessage() {
		return jobErrMessage;
	}
	/**
	 * @param jobErrMessage the jobErrMessage to set
	 */
	public void setJobErrMessage(String jobErrMessage) {
		this.jobErrMessage = jobErrMessage;
	}

}
