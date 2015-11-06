package com.haredb.harespark.bean.response;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class UploadDataFileStatusResponseBean extends ResponseInfoBean implements IResponseBean {

	public static String RUNNING = "RUNNING";
	public static String SUCCEEDED = "SUCCEEDED";
	public static String FAILED = "FAILED";

	private String startTime;
	private String finishTime;

	private String dataStatus;

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getFinishTime() {
		return finishTime;
	}

	public void setFinishTime(String finishTime) {
		this.finishTime = finishTime;
	}

	public String getDataStatus() {
		return dataStatus;
	}

	public void setDataStatus(String dataStatus) {
		this.dataStatus = dataStatus;
	}

	@Override
	public <T> Class<?> getBeanClass() {
		return this.getClass();
	}

	public String toString() {
		return new StringBuilder().append("UploadDataFile dataStatus : ").append(dataStatus)
				.append(", start time : ").append(startTime)
				.append(", finish time : ").append(finishTime).toString();	
	}
	
}
