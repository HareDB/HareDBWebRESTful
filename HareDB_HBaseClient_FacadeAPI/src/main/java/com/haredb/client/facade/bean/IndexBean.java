package com.haredb.client.facade.bean;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class IndexBean {

	private String tableName;
	private String collectionList;
	private String indexType;
	
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
	public String getCollectionList() {
		return collectionList;
	}
	public void setCollectionList(String collectionList) {
		this.collectionList = collectionList;
	}
	public String getIndexType() {
		return indexType;
	}
	public void setIndexType(String indexType) {
		this.indexType = indexType;
	}
	
	
	
}
