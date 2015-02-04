package com.haredb.client.facade.bean;

import java.util.List;

public class BulkTableBean {
	private boolean exist;
	private List<String> cfList;
	private String tableName;
	private boolean preSplit;
	private String compression;
	private String preSplitAlgo;
	private String startKey;
	private String endKey;
	private String rowKeyRange;
	private int regionCount;
	public boolean isExist() {
		return exist;
	}
	public void setExist(boolean exist) {
		this.exist = exist;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public boolean isPreSplit() {
		return preSplit;
	}
	public void setPreSplit(boolean preSplit) {
		this.preSplit = preSplit;
	}
	public String getCompression() {
		return compression;
	}
	public void setCompression(String compression) {
		this.compression = compression;
	}
	public String getPreSplitAlgo() {
		return preSplitAlgo;
	}
	public void setPreSplitAlgo(String preSplitAlgo) {
		this.preSplitAlgo = preSplitAlgo;
	}
	public String getStartKey() {
		return startKey;
	}
	public void setStartKey(String startKey) {
		this.startKey = startKey;
	}
	public String getEndKey() {
		return endKey;
	}
	public void setEndKey(String endKey) {
		this.endKey = endKey;
	}
	public String getRowKeyRange() {
		return rowKeyRange;
	}
	public void setRowKeyRange(String rowKeyRange) {
		this.rowKeyRange = rowKeyRange;
	}
	public int getRegionCount() {
		return regionCount;
	}
	public void setRegionCount(int regionCount) {
		this.regionCount = regionCount;
	}
	public List<String> getCfList() {
		return cfList;
	}
	public void setCfList(List<String> cfList) {
		this.cfList = cfList;
	}
}
