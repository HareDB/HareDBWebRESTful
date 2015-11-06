package com.haredb.harespark.bean.response;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class DescribeTableResponseBean extends ResponseInfoBean implements
		IResponseBean {

	private List<String> columnNames;
	private List<String> dataTypes;

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
