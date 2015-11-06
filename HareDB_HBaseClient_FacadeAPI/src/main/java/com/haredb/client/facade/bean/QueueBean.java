package com.haredb.client.facade.bean;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class QueueBean {
	private String tableName;
	private String queueFileName;
	

	public String getQueueFileName() {
		return queueFileName;
	}

	public void setQueueFileName(String queueFileName) {
		this.queueFileName = queueFileName;
	}

	private String resultPath;

	public String getResultPath() {
		return resultPath;
	}

	public void setResultPath(String resultPath) {
		this.resultPath = resultPath;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	

}
