package com.haredb.harespark.bean.input;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class UploadDataFileBean implements IInputBean {

	public static String operatorName_Normal = "Normal";
	public static String operatorName_Replace = "Replace";

	private String tableName;
	private String dataFilePath;
	private String resultPath;
	private String skipHeader;
	private String csvSeparator = ",";
	private String operator;

	public UploadDataFileBean() {

	}

	public UploadDataFileBean(String tableName, String dataFilePath, String resultPath, String skipHeader, String csvSeparator,
			String operator) {
		super();
		this.tableName = tableName;
		this.dataFilePath = dataFilePath;
		this.resultPath = resultPath;
		this.skipHeader = skipHeader;
		this.csvSeparator = csvSeparator;
		this.operator = operator;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getDataFilePath() {
		return dataFilePath;
	}

	public void setDataFilePath(String dataFilePath) {
		this.dataFilePath = dataFilePath;
	}

	public String getResultPath() {
		return resultPath;
	}

	public void setResultPath(String resultPath) {
		this.resultPath = resultPath;
	}

	public String getSkipHeader() {
		return skipHeader;
	}

	public void setSkipHeader(String skipHeader) {
		this.skipHeader = skipHeader;
	}

	public String getCsvSeparator() {
		return csvSeparator;
	}

	public void setCsvSeparator(String csvSeparator) {
		this.csvSeparator = csvSeparator;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}
}
