package com.haredb.harespark.bean.input;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class PreviewBean implements IInputBean {

	private String tableName;
	private String pageSize;
	private String limit;

	public PreviewBean() {
		
	}
	
	public PreviewBean(String tableName, String pageSize, String limit) {
		super();
		this.tableName = tableName;
		this.pageSize = pageSize;
		this.limit = limit;
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
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
