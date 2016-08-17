package com.haredb.client.listener;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.haredb.client.facade.until.HareEnv;
import com.haredb.client.facade.until.HareSparkSysConfig;



public class WebRestAppListener implements ServletContextListener{
	public static String HARESPARKCONFIGSTR = "haresparkenv";
	private static Logger logger = LoggerFactory.getLogger(WebRestAppListener.class);
	private ServletContext servletContext;
	@Override
	public void contextInitialized(ServletContextEvent sce) {
		logger.info("context init......");

		InputStream is = null;
		FileOutputStream fos = null;
		try {			
			ServletContext context = sce.getServletContext();
			File libFolder = new File(context.getRealPath("/WEB-INF/lib"));

			/**
			 * copy hadoop folder from resources
			 */
			logger.info("copy hadoop folder......");
			File hadoopFolder = HareEnv.getHadoopHomeDir();
			File resourceHadoopFolder = new File(this.getClass().getResource("/" + HareEnv.getHadoopFolderName()).getPath());
			FileUtils.copyDirectory(resourceHadoopFolder, hadoopFolder);
			
			/**
			 * modify HADOOP_CLASSPATH in hadoop-env.sh
			 */
			logger.info("modify HADOOP_CLASSPATH in hadoop-env.sh......");
			File hadoopEnvshFile = HareEnv.getHadoopEnvShell();
            BufferedReader br = new BufferedReader(new FileReader(hadoopEnvshFile));
            String line = null, data = "";
            boolean firstTag = false;
            while( (line=br.readLine()) != null ){ 
		         if(line != null && line.indexOf("export HADOOP_CLASSPATH=") != -1 && !firstTag){
		             String putData = "export HADOOP_CLASSPATH="+libFolder+"/*";
		             data += putData;
		             firstTag = true;
		         }else{
		        	 data += line;
		         }
		         data += "\n";
		    }
		    br.close();
            BufferedWriter bw = new BufferedWriter(new FileWriter(hadoopEnvshFile));
		    bw.write(data);
		    bw.flush();
		    bw.close();
		    
		    byte[] buf = new byte[1024];
		    int len;
		    /**
			 * copy bulkload jar from resources
			 * **/
			File bulkloadJar = new File(HareEnv.getBulkloadDir(), HareEnv.getbulkloadJarFileName());
			is = this.getClass().getResourceAsStream("/" + HareEnv.getbulkloadJarFileName());
			fos = new FileOutputStream(bulkloadJar);
			buf = new byte[is.available()];
		    
		    while ((len = is.read(buf)) > 0){
		    	fos.write(buf, 0, len);
		    }
			is.close();
			fos.close();
			
			/**
			 * copy index jar from resources
			 * **/
			File indexJar = HareEnv.getIndexJar();
			is = this.getClass().getResourceAsStream("/" + HareEnv.getIndexJarFileName());
			fos = new FileOutputStream(indexJar);
			buf = new byte[is.available()];
		    
		    while ((len = is.read(buf)) > 0){
		    	fos.write(buf, 0, len);
		    }
			is.close();
			fos.close();
		    
			/**
			 * 檢查作業系統是否為windows
			 * by jack
			 */
			String osName = System.getProperty("os.name");
			if(osName.startsWith("Windows")){
				logger.info("windows OS......");
				File hadoopDesFolder = HareEnv.getWinUtilsDir();
				System.setProperty("hadoop.home.dir", HareEnv.getWinHadoopHomeDir().getPath());
				
				File resourceWinUtilsFolder = new File(this.getClass().getResource("/" + HareEnv.getWinutilsfoldername()).getPath());

				for(File resourceWinUtilFile : resourceWinUtilsFolder.listFiles()){
					File desFile = new File(hadoopDesFolder.getPath() + "/" + resourceWinUtilFile.getName());
					if(!desFile.exists()){
						FileUtils.copyFile(resourceWinUtilFile, desFile);
					}
			    }
			}else{
				logger.info("linux OS......");
				Runtime.getRuntime().exec("chmod 777 " + hadoopEnvshFile.getPath());
			    Runtime.getRuntime().exec("chmod 777 " + HareEnv.getHadoopBin().getPath());
			    Runtime.getRuntime().exec("chmod 777 " + HareEnv.getHadoopCfg());
			}
			
			
			servletContext = sce.getServletContext();
			//HareSparkSysConfig sysConfig = new HareSparkSysConfig();
			//servletContext.setAttribute(HARESPARKCONFIGSTR, sysConfig);
			
			
		} catch (FileNotFoundException e) {
			logger.error("Coprocessor jar not found: "+e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}finally{
			try {
				if (fos != null)
					fos.close();
				if (is != null)
					is.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		logger.info("context destroyed......");		
	}

}
