package com.haredb.client.facade.bean;

import java.util.List;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement
public class MetaTableBean extends MessageInfo{

	private String hbaseTableName;
	private String metaTableName;
	private List<String> metaColumnNames;
	private List<String> hbaseColumnNames;
	private List<String> dataTypes;
	
	
	public String getHbaseTableName() {
		return hbaseTableName;
	}
	public void setHbaseTableName(String hbaseTableName) {
		this.hbaseTableName = hbaseTableName;
	}
	public String getMetaTableName() {
		return metaTableName;
	}
	public void setMetaTableName(String metaTableName) {
		this.metaTableName = metaTableName;
	}
	public List<String> getMetaColumnNames() {
		return metaColumnNames;
	}
	public void setMetaColumnNames(List<String> metaColumnNames) {
		this.metaColumnNames = metaColumnNames;
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
