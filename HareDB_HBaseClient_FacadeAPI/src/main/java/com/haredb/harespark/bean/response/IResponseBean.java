package com.haredb.harespark.bean.response;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public interface IResponseBean {
	public <T> Class<?> getBeanClass();
}
