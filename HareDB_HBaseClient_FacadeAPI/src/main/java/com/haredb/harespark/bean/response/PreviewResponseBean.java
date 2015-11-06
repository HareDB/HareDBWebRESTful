package com.haredb.harespark.bean.response;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class PreviewResponseBean extends ResponseInfoBean implements
		IResponseBean {

	private String[] heads;
	private String[][] results;

	public String[] getHeads() {
		return heads;
	}

	public void setHeads(String[] heads) {
		this.heads = heads;
	}

	public String[][] getResults() {
		return results;
	}

	public void setResults(String[][] results) {
		this.results = results;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}
	
}
