package com.haredb.harespark.bean.response;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class QueryStatusResponseBean extends ResponseInfoBean implements IResponseBean {

	public static String RUNNING = "RUNNING";
	public static String PREP = "PREP";
	public static String FINISHED = "FINISHED";
	public static String FAILED = "FAILED";
	public static String KILLED = "KILLED";

	private String jobID;
	private String jobName;
	private String jobStartTime;
	private String jobFinishTime;
	private String jobStatus;
	private String jobProgress;

	private String jobErrMessage;

	public String getJobID() {
		return jobID;
	}

	public void setJobID(String jobID) {
		this.jobID = jobID;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getJobStartTime() {
		return jobStartTime;
	}

	public void setJobStartTime(String jobStartTime) {
		this.jobStartTime = jobStartTime;
	}

	public String getJobFinishTime() {
		return jobFinishTime;
	}

	public void setJobFinishTime(String jobFinishTime) {
		this.jobFinishTime = jobFinishTime;
	}

	public String getJobStatus() {
		return jobStatus;
	}

	public void setJobStatus(String jobStatus) {
		this.jobStatus = jobStatus;
	}

	public String getJobProgress() {
		return jobProgress;
	}

	public void setJobProgress(String jobProgress) {
		this.jobProgress = jobProgress;
	}

	public String getJobErrMessage() {
		return jobErrMessage;
	}

	public void setJobErrMessage(String jobErrMessage) {
		this.jobErrMessage = jobErrMessage;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}

	public String toString() {		
		return new StringBuilder().append("jobID : ").append(jobID)
				.append(", jobID : ").append(jobID)
				.append(", jobStartTime : ").append(jobStartTime)
				.append(", jobFinishTime : ").append(jobFinishTime)
				.append(", jobStatus : ").append(jobStatus)
				.append(", jobProgress : ").append(jobProgress)
				.append(", jobErrMessage").append(jobErrMessage).toString();
	}
	
	
	
}
