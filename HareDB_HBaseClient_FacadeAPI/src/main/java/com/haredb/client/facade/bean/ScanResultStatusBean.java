package com.haredb.client.facade.bean;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ScanResultStatusBean extends MessageInfo{
	private String[][] results;
	private List<DataCellBean> heads;

	public String[][] getResults() {
		return results;
	}

	public void setResults(String[][] results) {
		this.results = results;
	}

	public List<DataCellBean> getHeads() {
		return heads;
	}

	public void setHeads(List<DataCellBean> heads) {
		this.heads = heads;
	}
}
