package com.haredb.client.facade.bean;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class UploadSchemaBean {
	private String schemaFilePath;
	private String dataPath;
	private String resultPath;
	public String getSchemaFilePath() {
		return schemaFilePath;
	}
	public void setSchemaFilePath(String schemaFilePath) {
		this.schemaFilePath = schemaFilePath;
	}
	public String getDataPath() {
		return dataPath;
	}
	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}
	public String getResultPath() {
		return resultPath;
	}
	public void setResultPath(String resultPath) {
		this.resultPath = resultPath;
	}

}
