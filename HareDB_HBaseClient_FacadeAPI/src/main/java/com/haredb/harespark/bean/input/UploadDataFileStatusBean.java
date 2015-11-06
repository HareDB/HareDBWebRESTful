package com.haredb.harespark.bean.input;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class UploadDataFileStatusBean implements IInputBean {


	private String uploadJobName;

	
	public UploadDataFileStatusBean() {
		
	}
	
	public UploadDataFileStatusBean(String tablename, String dataFileName, String uploadJobName) {
		super();
		this.uploadJobName = uploadJobName;
	}
	
	

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
