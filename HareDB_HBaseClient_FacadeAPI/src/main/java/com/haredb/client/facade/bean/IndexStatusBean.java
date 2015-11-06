package com.haredb.client.facade.bean;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class IndexStatusBean extends MessageInfo{
	private String indexStartTime;
	private String indexFinishTime;
	
	private String jobName;

	
	
	
	public String getIndexStartTime() {
		return indexStartTime;
	}

	public void setIndexStartTime(String indexStartTime) {
		this.indexStartTime = indexStartTime;
	}

	public String getIndexFinishTime() {
		return indexFinishTime;
	}

	public void setIndexFinishTime(String indexFinishTime) {
		this.indexFinishTime = indexFinishTime;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	
	
}
