package com.haredb.client.facade.bean;

import java.util.List;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement
public class MetaTableBean extends MessageInfo{

	private String hbaseTableName;
	private List<String> hiveColumnNames;
	private List<String> hbaseColumnNames;
	private List<String> dataTypes;
	
	
	public String getHbaseTableName() {
		return hbaseTableName;
	}
	public void setHbaseTableName(String hbaseTableName) {
		this.hbaseTableName = hbaseTableName;
	}
	public List<String> getHiveColumnNames() {
		return hiveColumnNames;
	}
	public void setHiveColumnNames(List<String> hiveColumnNames) {
		this.hiveColumnNames = hiveColumnNames;
	}
	public List<String> getHbaseColumnNames() {
		return hbaseColumnNames;
	}
	public void setHbaseColumnNames(List<String> hbaseColumnNames) {
		this.hbaseColumnNames = hbaseColumnNames;
	}
	public List<String> getDataTypes() {
		return dataTypes;
	}
	public void setDataTypes(List<String> dataTypes) {
		this.dataTypes = dataTypes;
	}
	
}
