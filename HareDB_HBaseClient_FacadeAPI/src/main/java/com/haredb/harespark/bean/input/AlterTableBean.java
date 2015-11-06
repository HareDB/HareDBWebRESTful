package com.haredb.harespark.bean.input;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class AlterTableBean implements IInputBean {

	
	private String tableName;
	private List<String> columnNames;
	private List<String> dataTypes;
	
	public AlterTableBean() {
		
	}
	
	public AlterTableBean(String tableName, List<String> columnNames, List<String> dataTypes) {
		super();
		this.tableName = tableName;
		this.columnNames = columnNames;
		this.dataTypes = dataTypes;
	}
	
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
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
