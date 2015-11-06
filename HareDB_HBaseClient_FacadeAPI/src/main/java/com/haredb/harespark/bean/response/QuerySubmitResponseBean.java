package com.haredb.harespark.bean.response;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class QuerySubmitResponseBean extends ResponseInfoBean implements IResponseBean {

	private String queryJobName;

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
