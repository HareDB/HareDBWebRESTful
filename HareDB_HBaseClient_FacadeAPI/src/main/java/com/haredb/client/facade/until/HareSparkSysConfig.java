package com.haredb.client.facade.until;

import java.io.InputStream;
import java.util.Properties;

import com.haredb.harespark.common.Constants;
import com.haredb.harespark.common.SysConfig;

public class HareSparkSysConfig {
	private SysConfig sysConfig;
	
	public HareSparkSysConfig(){
		try{
			SysConfig sysConfig = new SysConfig();
			
			InputStream inStream = this.getClass().getResourceAsStream(Constants.sysConfigFilePath);
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
				throw new RuntimeException(Constants.sysConfigFilePath + " file is not found, please set CLASSPATH");
			}
		    
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	public SysConfig getSysConfig() {
		return sysConfig;
	}

	public void setSysConfig(SysConfig sysConfig) {
		this.sysConfig = sysConfig;
	}

}
