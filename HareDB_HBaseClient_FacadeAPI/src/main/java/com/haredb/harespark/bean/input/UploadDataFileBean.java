package com.haredb.harespark.bean.input;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class UploadDataFileBean implements IInputBean {

	public static String operatorName_Normal = "Normal";
	public static String operatorName_Replace = "Replace";

	private String tablename;
	private String dataFilePath;
	private String resultPath;
	private String skipHeader;
	private String csvSeparator = ",";
	private String operator;

	public UploadDataFileBean() {

	}

	public UploadDataFileBean(String tablename, String dataFilePath, String resultPath, String skipHeader, String csvSeparator,
			String operator) {
		super();
		this.tablename = tablename;
		this.dataFilePath = dataFilePath;
		this.resultPath = resultPath;
		this.skipHeader = skipHeader;
		this.csvSeparator = csvSeparator;
		this.operator = operator;
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tableName) {
		this.tablename = tableName;
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
