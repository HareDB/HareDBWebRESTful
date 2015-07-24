package com.haredb.client.facade.bean;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class HareQLResultStatusBean extends MessageInfo{
	private Long rowSize;
	private Long fileSize;
	private List<String> results;
	private List<String> heads;
	private String status;
	private String exception;
	
	
	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}
	
	public Long getRowSize() {
		return rowSize;
	}
	public void setRowSize(Long rowSize) {
		this.rowSize = rowSize;
	}
	public Long getFileSize() {
		return fileSize;
	}
	public void setFileSize(Long fileSize) {
		this.fileSize = fileSize;
	}
	public List<String> getResults() {
		return results;
	}
	public void setResults(List<String> results) {
		this.results = results;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public List<String> getHeads() {
		return heads;
	}
	public void setHeads(List<String> heads) {
		this.heads = heads;
	}
	public String getException() {
		return exception;
	}
	public void setException(String exception) {
		this.exception = exception;
	}
	
	

}
