package com.haredb.harespark.operator;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.haredb.harespark.bean.input.CreateTableBean;
import com.haredb.harespark.bean.input.DropTableBean;
import com.haredb.harespark.bean.input.UserSessionBean;
import com.haredb.harespark.bean.response.ResponseInfoBean;

public class HareSparkOperatorTest {

	private HareSparkOperator operator;
	private String testingTableName = "HareSparkOperatorTest";
	
	@Test
	public void testCreateUserSession() {
		operator = new HareSparkOperator();
		
		ResponseInfoBean bean = operator.createUserSession(getUserSessionBean());		
		assertEquals(bean.getStatus(),bean.SUCCESS);
		
		UserSessionBean session = getUserSessionBean();
		session.setConfigurationFolderPath("/home/ssss/");
		bean = operator.createUserSession(session);
		assertEquals(bean.getStatus(),bean.ERROR);
		
	}

	
	
	
	private UserSessionBean getUserSessionBean() {
		UserSessionBean bean = new UserSessionBean();
		ClassLoader classLoader = getClass().getClassLoader();		
		File file = new File(classLoader.getResource("config").getFile());		
		bean.setConfigurationFolderPath(file.getAbsolutePath());		
		return bean;
	}
	
	
}
