package com.haredb.harespark.bean.input;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class DescribeTableBean implements IInputBean {

	private String tablename;

	public DescribeTableBean() {
		
	}
	public DescribeTableBean(String tablename) {
		super();
		this.tablename = tablename;
	}
	
	
	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}

}
