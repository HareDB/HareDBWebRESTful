package com.haredb.client.facade.bean;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class MessageInfo implements IBean{

	public static final String ERROR="error";
	public static final String SUCCESS="success";
	public static final String OTHER="other";
	public static final String RUNNING="running";
	
	protected long responseTime;
	protected String status;
	protected String exception;
	protected String connectionKey;
	
	
	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}
	
	/**
	 * do nothing,always return true.
	 */
	protected String auth = "true";
	public String getAuth() {
		return auth;
	}
	public void setAuth(String auth) {
		this.auth = auth;
	}
	
	public long getResponseTime() {
		return responseTime;
	}
	public void setResponseTime(long responseTime) {
		this.responseTime = responseTime;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getException() {
		return exception;
	}
	public void setException(String exception) {
		this.exception = exception;
	}
	public String getConnectionKey() {
		return connectionKey;
	}
	public void setConnectionKey(String connectionKey) {
		this.connectionKey = connectionKey;
	}
}
