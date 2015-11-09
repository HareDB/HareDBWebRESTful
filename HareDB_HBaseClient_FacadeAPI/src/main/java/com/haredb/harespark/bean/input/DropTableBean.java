package com.haredb.harespark.bean.input;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class DropTableBean implements IInputBean {

	private String tablename;
	
	public DropTableBean() {
		
	}
	
	public DropTableBean(String tablename) {
		super();
		this.tablename = tablename;
	}
	
	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tableName) {
		this.tablename = tableName;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}

}
