package com.haredb.client.facade.bean;

import java.util.List;

import com.haredb.hbase.bulkload.domain.BulkColumn;

public class BulkloadBean{
	private String bulkloadType;
	private List<BulkColumn> columns;
	private long timestamp;
	private boolean skipBadLine;
	private String jarPath;
	private String jobName;
	/**
	 * @return the bulkloadType
	 */
	public String getBulkloadType() {
		return bulkloadType;
	}
	/**
	 * @param bulkloadType the bulkloadType to set
	 */
	public void setBulkloadType(String bulkloadType) {
		this.bulkloadType = bulkloadType;
	}
	/**
	 * @return the columns
	 */
	public List<BulkColumn> getColumns() {
		return columns;
	}
	/**
	 * @param columns the columns to set
	 */
	public void setColumns(List<BulkColumn> columns) {
		this.columns = columns;
	}
	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}
	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	/**
	 * @return the skipBadLine
	 */
	public boolean isSkipBadLine() {
		return skipBadLine;
	}
	/**
	 * @param skipBadLine the skipBadLine to set
	 */
	public void setSkipBadLine(boolean skipBadLine) {
		this.skipBadLine = skipBadLine;
	}
	/**
	 * @return the jarPath
	 */
	public String getJarPath() {
		return jarPath;
	}
	/**
	 * @param jarPath the jarPath to set
	 */
	public void setJarPath(String jarPath) {
		this.jarPath = jarPath;
	}
	/**
	 * @return the jobName
	 */
	public String getJobName() {
		return jobName;
	}
	/**
	 * @param jobName the jobName to set
	 */
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}	
}