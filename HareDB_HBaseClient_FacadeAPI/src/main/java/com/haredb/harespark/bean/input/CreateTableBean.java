package com.haredb.harespark.bean.input;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class CreateTableBean implements IInputBean {

	
	
	private String tableName;
	private List<String> columnNames;
	private List<String> dataType;
	
	public CreateTableBean() {
		
	}
	
	public CreateTableBean(String tableName, List<String> columnNames, List<String> dataType) {
		super();
		this.tableName = tableName;
		this.columnNames = columnNames;
		this.dataType = dataType;
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

	public List<String> getDataType() {
		return dataType;
	}

	public void setDataType(List<String> dataType) {
		this.dataType = dataType;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}

}
