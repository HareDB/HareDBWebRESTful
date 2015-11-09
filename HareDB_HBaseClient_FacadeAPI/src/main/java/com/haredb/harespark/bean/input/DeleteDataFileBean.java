package com.haredb.harespark.bean.input;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class DeleteDataFileBean implements IInputBean {

	private String tablename;
	private String deleteDataFileName;

	public DeleteDataFileBean() {
		
	}
	
	public DeleteDataFileBean(String tablename, String deleteDataFileName) {
		super();
		this.tablename = tablename;
		this.deleteDataFileName = deleteDataFileName;
	}
	
	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tableName) {
		this.tablename = tableName;
	}

	public String getDeleteDataFileName() {
		return deleteDataFileName;
	}

	public void setDeleteDataFileName(String deleteDataFileName) {
		this.deleteDataFileName = deleteDataFileName;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}
}
