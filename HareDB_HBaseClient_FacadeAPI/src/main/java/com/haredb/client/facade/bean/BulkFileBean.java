package com.haredb.client.facade.bean;

public class BulkFileBean {
	private String fileType;
	private String fileLocation;
	private String filePath;
	private String outputFolder;
	private boolean existHeader;
	private String separator;
	private String logFilePath;
	private String infoFilePath;
	private String bkResultFilePath;
	
	/**
	 * path of bulkload result that writing to file
	 * @return the bkResultFilePath
	 */
	public String getBkResultFilePath() {
		return bkResultFilePath;
	}
	/**path of bulkload result that writing to file
	 * @param bkResultFilePath the bkResultFilePath to set
	 */
	public void setBkResultFilePath(String bkResultFilePath) {
		this.bkResultFilePath = bkResultFilePath;
	}
	public String getFileType() {
		return fileType;
	}
	public void setFileType(String fileType) {
		this.fileType = fileType;
	}
	public String getFileLocation() {
		return fileLocation;
	}
	public void setFileLocation(String fileLocation) {
		this.fileLocation = fileLocation;
	}
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	public String getOutputFolder() {
		return outputFolder;
	}
	public void setOutputFolder(String outputFolder) {
		this.outputFolder = outputFolder;
	}
	public boolean isExistHeader() {
		return existHeader;
	}
	public void setExistHeader(boolean existHeader) {
		this.existHeader = existHeader;
	}
	public String getSeparator() {
		return separator;
	}
	public void setSeparator(String separator) {
		this.separator = separator;
	}
	public String getLogFilePath() {
		return logFilePath;
	}
	public void setLogFilePath(String logFilePath) {
		this.logFilePath = logFilePath;
	}
	public String getInfoFilePath() {
		return infoFilePath;
	}
	public void setInfoFilePath(String infoFilePath) {
		this.infoFilePath = infoFilePath;
	}
}
