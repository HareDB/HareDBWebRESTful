package com.haredb.harespark.operator;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;

import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.harespark.bean.input.UserSessionBean;
import com.haredb.harespark.bean.response.ResponseInfoBean;

public class HareSparkOperatorTest {

	private HareSparkOperator operator;
	private String testingTableName = "HareSparkOperatorTest";
	
	@Test
	public void testCreateUserSession() {
		operator = new HareSparkOperator();
		
		ResponseInfoBean bean = operator.createUserSession(getUserSessionBean());		

		assertEquals(bean.getStatus(),MessageInfo.SUCCESS);
		
		UserSessionBean session = getUserSessionBean();
		session.setConfigurationFolderPath("/home/ssss/");
		bean = operator.createUserSession(session);
		assertEquals(bean.getStatus(),MessageInfo.ERROR);
		
	}

	
	
	
	private UserSessionBean getUserSessionBean() {
		UserSessionBean bean = new UserSessionBean();
		ClassLoader classLoader = getClass().getClassLoader();		
		File file = new File(classLoader.getResource("config").getFile());		
		bean.setConfigurationFolderPath(file.getAbsolutePath());		
		return bean;
	}
	
	
}
