package com.haredb.harespark.bean.input;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class AlterTableBean implements IInputBean {

	
	private String tablename;
	private List<String> columnNames;
	private List<String> dataTypes;
	
	public AlterTableBean() {
		
	}
	
	public AlterTableBean(String tablename, List<String> columnNames, List<String> dataTypes) {
		super();
		this.tablename = tablename;
		this.columnNames = columnNames;
		this.dataTypes = dataTypes;
	}
	
	
	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tableName) {
		this.tablename = tableName;
	}
	
	public List<String> getColumnNames() {
		return columnNames;
	}
	
	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}

	public List<String> getDataTypes() {
		return dataTypes;
	}

	public void setDataTypes(List<String> dataTypes) {
		this.dataTypes = dataTypes;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}

}
