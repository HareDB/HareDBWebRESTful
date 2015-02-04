package com.haredb.client.facade.operator;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.haredb.client.facade.bean.IBean;
import com.haredb.hbaseclient.core.Connection;

/**
 * Haredb Contrivance class
 * @author stana
 *
 */
public abstract class HareContrivance {

	protected Connection connection;
	private Configuration config;
	
	public HareContrivance(Connection connection){
		this.connection = connection;
		this.config = new Configuration();
		this.config.set(Connection.nameNodeHostPortString, connection.getNameNodeHostPort());
	}
		
	protected boolean checkNull(Object t) {
		boolean result = false;
		if(t == null) {
			result = true;
		}
		return result;
	}
	
	/**
	 * @param object
	 * @param filePathWithName
	 * @param overwriteFile
	 */
	public void writeFileToHdfs(IBean object,String filePathWithName, boolean overwriteFile){
		String filename = filePathWithName.substring(filePathWithName.lastIndexOf('/') + 1,filePathWithName.length());
		String filePath = filePathWithName.replace("/"+filename, "");
		this.writeFileToHdfs(object,filePath,filename,overwriteFile);
	}
	
	/**
	 * @param object :result bean of writing file 
	 * @param savedPath :path of file saved
	 * @param fileName :file name
	 * @param overwriteFile :overwrite file
	 */
	public void writeFileToHdfs(IBean object, String savedPath,String fileName, boolean overwriteFile){
		byte[] byt= null;
		Path filePath = new Path(savedPath+"/"+fileName);
		FSDataOutputStream fsOutStream;
		String propertyName = null;
		Object value = null;
		try {
			FileSystem hdfs = FileSystem.get(config);
			BeanInfo beanInfo = Introspector.getBeanInfo(object.getBeanClass());
			if(hdfs.exists(filePath)){
				if(overwriteFile) {
					hdfs.delete(filePath, true);
					fsOutStream = hdfs.create(filePath);
				} else {
					fsOutStream = hdfs.append(filePath);
				}
			} else {
				fsOutStream = hdfs.create(filePath);
			}
			
			for (PropertyDescriptor propertyDesc : beanInfo.getPropertyDescriptors()) {
			    propertyName = propertyDesc.getName();
			    if(isOutputToFile(propertyName)) {
			    	value = propertyDesc.getReadMethod().invoke(object);
				    if(value != null) {
				    	byt = (propertyName + ":" + value+"\n").getBytes();
						fsOutStream.write(byt, 0, byt.length);//wrap
				    }
			    }
			}
			fsOutStream.close();
			hdfs.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}

	/**
	 * check property of BeanInfo
	 * @param propertyName
	 * @return
	 */
	private boolean isOutputToFile(String propertyName){
		boolean result = false;
		if(propertyName.equals("heads") == false && 
	    		propertyName.equals("results")  == false && 
	    		propertyName.equals("beanClass")  == false && 
	    		propertyName.equals("class") == false) {
			result = true;
		}
		return result;
	}
	
}
