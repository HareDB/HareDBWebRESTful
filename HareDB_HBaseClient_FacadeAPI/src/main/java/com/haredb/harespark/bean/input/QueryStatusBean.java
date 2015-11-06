package com.haredb.harespark.bean.input;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class QueryStatusBean implements IInputBean {

	private String queryJobName;

	public QueryStatusBean() {
		
	}
	
	public QueryStatusBean(String queryJobName) {
		super();
		this.queryJobName = queryJobName;
	}
	
	public String getQueryJobName() {
		return queryJobName;
	}

	public void setQueryJobName(String queryJobName) {
		this.queryJobName = queryJobName;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}

}
