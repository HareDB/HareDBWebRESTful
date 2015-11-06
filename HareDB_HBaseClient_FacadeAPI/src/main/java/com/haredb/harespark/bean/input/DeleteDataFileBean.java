package com.haredb.harespark.bean.input;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class DeleteDataFileBean implements IInputBean {

	private String tableName;
	private String deleteDataFileName;

	public DeleteDataFileBean() {
		
	}
	
	public DeleteDataFileBean(String tableName, String deleteDataFileName) {
		super();
		this.tableName = tableName;
		this.deleteDataFileName = deleteDataFileName;
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
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
