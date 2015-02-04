package com.haredb.client.facade.bean;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TableInfoBean {
	private String tableName;
	private List<String> columnFamilys;
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public List<String> getColumnFamilys() {
		return columnFamilys;
	}
	public void setColumnFamilys(List<String> columnFamilys) {
		this.columnFamilys = columnFamilys;
	}
}
