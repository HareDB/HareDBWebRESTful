package com.haredb.client.facade.bean;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class HareQLQueryBean{

	private String tempFilePath;
	private String sql;
	private int page = 0;
	private int limit = 0;
	public String getTempFilePath() {
		return tempFilePath;
	}
	public void setTempFilePath(String tempFilePath) {
		this.tempFilePath = tempFilePath;
	}
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public int getPage() {
		return page;
	}
	public void setPage(int page) {
		this.page = page;
	}
	public int getLimit() {
		return limit;
	}
	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	
}
