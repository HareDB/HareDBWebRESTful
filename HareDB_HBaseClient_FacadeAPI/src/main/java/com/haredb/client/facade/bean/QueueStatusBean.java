package com.haredb.client.facade.bean;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class QueueStatusBean extends MessageInfo{

	private String tableName;
	private String queueStatus;
	private List<String> queueFiles;
	
	public String getQueueStatus() {
		return queueStatus;
	}

	public void setQueueStatus(String queueStatus) {
		this.queueStatus = queueStatus;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<String> getQueueFiles() {
		return queueFiles;
	}

	public void setQueueFiles(List<String> queueFiles) {
		this.queueFiles = queueFiles;
	}


	
}
