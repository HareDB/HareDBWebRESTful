package com.haredb.harespark.bean.input;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class DescribeTableBean implements IInputBean {

	private String tableName;

	public DescribeTableBean() {
		
	}
	public DescribeTableBean(String tableName) {
		super();
		this.tableName = tableName;
	}
	
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}

}
