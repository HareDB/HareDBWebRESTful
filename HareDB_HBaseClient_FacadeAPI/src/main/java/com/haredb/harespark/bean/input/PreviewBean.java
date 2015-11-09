package com.haredb.harespark.bean.input;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class PreviewBean implements IInputBean {

	private String tablename;
	private String pageSize;
	private String limit;

	public PreviewBean() {
		
	}
	
	public PreviewBean(String tablename, String pageSize, String limit) {
		super();
		this.tablename = tablename;
		this.pageSize = pageSize;
		this.limit = limit;
	}
	
	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tableName) {
		this.tablename = tableName;
	}

	public String getPageSize() {
		return pageSize;
	}

	public void setPageSize(String pageSize) {
		this.pageSize = pageSize;
	}

	public String getLimit() {
		return limit;
	}

	public void setLimit(String limit) {
		this.limit = limit;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}

}
