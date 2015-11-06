package com.haredb.harespark.bean.input;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class UserSessionBean implements IInputBean {

	public static String sessionKey = "usersession";
	
	private String configurationFolderPath;	
	
	public String getConfigurationFolderPath() {
		return configurationFolderPath;
	}

	public void setConfigurationFolderPath(String configurationFolderPath) {
		this.configurationFolderPath = configurationFolderPath;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}
	
	
}
