package com.haredb.harespark.bean.input;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public interface IInputBean {
	public <T> Class<?> getBeanClass();
}
