package com.haredb.harespark.bean.input;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class QuerySubmitBean implements IInputBean {

	
	private String sql;
	private String tablename;
	private String resultFileFolder;
		
	public QuerySubmitBean() {
		
	}
	
	public QuerySubmitBean(String sql, String tablename, String resultFileFolder) {
		super();
		this.sql = sql;
		this.tablename = tablename;
		this.resultFileFolder = resultFileFolder;
	}
	
	
	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public String getResultFileFolder() {
		return resultFileFolder;
	}

	public void setResultFileFolder(String resultFileFolder) {
		this.resultFileFolder = resultFileFolder;
	}



	@Override
	public <T> Class<?> getBeanClass() {		
		return this.getClass();
	}

}
