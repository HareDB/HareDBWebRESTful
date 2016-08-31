package com.haredb.client.facade.until;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.haredb.harespark.common.Constants;
import com.haredb.harespark.common.SysConfig;

public class HareSparkSysConfig {
	private static Logger logger = LoggerFactory.getLogger(HareSparkSysConfig.class);
	private SysConfig sysConfig;
	
	public HareSparkSysConfig(){
		InputStream inStream =null;
		try{
			SysConfig sysConfig = new SysConfig();
			
			 inStream = this.getClass().getResourceAsStream(Constants.sysConfigFilePath);
			if(inStream != null){
				Properties properties = new Properties();
				properties.load(inStream);
				
				sysConfig.setProductName(properties.getProperty(Constants.PRODUCTNAME));
				sysConfig.setHdfsDataRoot(properties.getProperty(Constants.HDFSTABLEFOLDER));
				sysConfig.setMetaFolderName(properties.getProperty(Constants.METAFOLDERNAME));
				sysConfig.setSparkAssemblyJar(properties.getProperty(Constants.SPARKASSEMBLYJAR));
				sysConfig.setHareSparkAssemblyPath(properties.getProperty(Constants.HARESPARKASSEMBLYPATH));
				sysConfig.setSparkcsvJarPath(properties.getProperty(Constants.SPARKCSVJARPATH));
				sysConfig.setSparkcommoncsvJarPath(properties.getProperty(Constants.SPARKCOMMONCSVJARPATH));
			    this.setSysConfig(sysConfig);
			}else{
				logger.info("If your use HareSpark, please setting " + Constants.sysConfigFilePath + " classpath");
				logger.info(Constants.sysConfigFilePath + " file is not found, please set CLASSPATH");
			}
		    
		}catch(Exception e){
			throw new RuntimeException(e);
		}finally{
			if(inStream != null){
				try {
					inStream.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	public SysConfig getSysConfig() {
		return sysConfig;
	}

	public void setSysConfig(SysConfig sysConfig) {
		this.sysConfig = sysConfig;
	}

}
