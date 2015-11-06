package com.haredb.harespark.bean.response;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class UploadDataFileResponseBean extends ResponseInfoBean implements IResponseBean {

	private String uploadJobName;

	public String getUploadJobName() {
		return uploadJobName;
	}

	public void setUploadJobName(String uploadJobName) {
		this.uploadJobName = uploadJobName;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}

}
