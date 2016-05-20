package com.haredb.harespark.bean.response;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ResponseInfoBean implements IResponseBean {

	/*public final String ERROR = "error";
	public final String SUCCESS = "success";
	public final String OTHER = "other";
	public final String RUNNING = "running";*/

	private long responseTime;
	private String status;
	private String exception;
	private String connectionKey;
	private String auth = "true";

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

	public String getAuth() {
		return auth;
	}

	public void setAuth(String auth) {
		this.auth = auth;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}


}
