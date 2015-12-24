package com.haredb.client.facade.until;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

/**
 * @author allen
 *
 */
public class HareEnv {
	
	private static int NODE_COUNT = -1; 
	
	private final static String ROOT = "exec/";
	
	private final static String winUtilsFolderName = "winHadoop/bin";
	
	private final static String hadoopFolderName   = "hadoop";
	
	private final static String connectionFileName = "connection.xml";
	
//	private final static String coprocessorJar     = "haredbcoprocessor" + getVersion() + ".jar";
	
	private final static String bulkloadJar        = "HareDB_Bulkload.jar";
	
	private final static String indexJar           = "HareDB_Index.jar";
	
	private final static String verificationFileName = ".haredb.ah"; 
	
	public final static String WINUTILS = "exec/windows/hadoop/bin";
	
	public final static String WIN_HADOOPHOME = "exec/windows/hadoop";
	
	public final static String  CONNECTION = "exec/connections";
	
	public final static String HQL_HISTORY = "exec/history";
	
	public final static String COPROCESSOR = "exec/coprocessor";
	
	public final static String    BULKLOAD = "exec/bulkload";
	
	public final static String    BULKLOAD_LOGS = "exec/bulkload/logs";
	
	private final static String INFO_FILE = "/info.properties";
	
	public final static String HIVE_EXEC_SCRATCHDIR = "exec/tmp/logs";
	
	public final static String HADOOP_HOME = "exec/hadoop"; 
	
	private final static String HADOOP_BIN = "exec/hadoop/bin/hadoop";
	
	private final static String HADOOP_CFG = "exec/hadoop/libexec/hadoop-config.sh";
	
	private final static String HADOOP_ENV_SHELL = HADOOP_HOME+"/etc/hadoop/hadoop-env.sh";
	
	public final static String INDEX = "exec/index";
	
	public final static String INDEX_ZK_TMP = "exec/index/zkTmp";
	
	public final static String[]     PATHS = new String[]{CONNECTION, 
													      HQL_HISTORY,
													      COPROCESSOR, 
													      BULKLOAD, 
													      BULKLOAD_LOGS,
													      WINUTILS,
													      INDEX};
	
	static {
		File file = null;
		for(String path: PATHS){
			file = new File(getHomeDir(), path);
			if(!file.exists()) file.mkdirs();
		}
	}
	
	public HareEnv() {
		// TODO Auto-generated constructor stub
	}
	
	/*
	 * Return a File instance for the connection folder %HARE_HOME%/exec/connections/ 
	 * 
	 * */
	public static File getConnDir(){
		return new File(HareEnv.getHomeDir(), HareEnv.CONNECTION);
	}
	
	public static File getCoprocessorDir(){
		return new File(HareEnv.getHomeDir(), HareEnv.COPROCESSOR);
	}
	
	public static File getHqlHistoryDir(){
		return new File(HareEnv.getHomeDir(), HareEnv.HQL_HISTORY);
	}
	
	public static File getBulkloadDir(){
		return new File(HareEnv.getHomeDir(), HareEnv.BULKLOAD);
	}
	
	public static File getBulkloadLogDir(){
		return new File(HareEnv.getHomeDir(), HareEnv.BULKLOAD_LOGS);
	}
	
	public static File getIndexDir(){
		return new File(HareEnv.getHomeDir(), HareEnv.INDEX);
	}
	
	public static File getIndexTempDir(){
		return new File(HareEnv.getHomeDir(), HareEnv.INDEX_ZK_TMP);
	}
	
	public static File getWinUtilsDir(){
		return new File(HareEnv.getHomeDir(), HareEnv.WINUTILS);
	}
	
	public static File getWinHadoopHomeDir(){
		return new File(HareEnv.getHomeDir(), HareEnv.WIN_HADOOPHOME);
	}

	public static File getHadoopHomeDir(){
		return new File(HareEnv.getHomeDir(), HareEnv.HADOOP_HOME);
	}
	
	public static File getRootDir(){
		return new File(HareEnv.getHomeDir(), HareEnv.ROOT);
	}
	
	public static File getHadoopBin(){
		return new File(HareEnv.getHomeDir(), HareEnv.HADOOP_BIN);
	}
	
	public static File getHadoopCfg(){
		return new File(HareEnv.getHomeDir(), HareEnv.HADOOP_CFG);
	}
	
	public static File getHadoopEnvShell(){
		return new File(HareEnv.getHomeDir(), HareEnv.HADOOP_ENV_SHELL);
	}
	
	public static File getIndexJar(){
		return new File(HareEnv.getIndexDir(), HareEnv.getIndexJarFileName());
	}
	
	public static File getBulkloadJar(){
		return new File(HareEnv.getBulkloadDir(), HareEnv.getbulkloadJarFileName());
	}
	
	public static boolean isWindows(){
		return System.getProperty("os.name").toLowerCase().indexOf("win") >= 0;
	}
	
    public static File getHomeDir() {

        // Check HAREDB_HOME  system property
        String hudsonHomeProperty = System.getProperty("HAREDB_HOME");
        if (hudsonHomeProperty != null) {
            return new File(hudsonHomeProperty.trim());
        }

        // Check if the environment variable is et
        try {
            String hudsonHomeEnv = System.getenv("HAREDB_HOME");
            if (hudsonHomeEnv != null) {
                return new File(hudsonHomeEnv.trim()).getAbsoluteFile();
            }
        } catch (Throwable _) {
            // Some JDK could throw error if HAREDB_HOME is not set.
            // Ignore and fall through
        }

        // Default hare home 
        return new File(new File(System.getProperty("user.home")), ".haredb");
    }
    
    /**
     * connection file name
     * @return
     */
    public static String getConnectionFileName() {
    	return connectionFileName;
    }
    
//    public static String getCoprocessorJarFileName(){
//    	return coprocessorJar;
//    }
    
    public static String getbulkloadJarFileName(){
    	return bulkloadJar;
    }
    
    public static String getIndexJarFileName(){
    	return indexJar;
    }

    public static String getWinutilsfoldername() {
		return winUtilsFolderName;
	}
    
    public static String getHadoopFolderName(){
    	return hadoopFolderName;
    }

    
    public static String isSourceforge(){
    	try{
	    	InputStream inStream = HareEnv.class.getResourceAsStream(INFO_FILE);
	    	Properties systemInfoProp = new Properties();
	    	systemInfoProp.load(inStream);
	    	String passFilter = systemInfoProp.get("passFilter").toString();
	    	return passFilter;
	    	
    	}catch(Exception e){
    		throw new RuntimeException(e);
    	}
    }
    
	
	public static int getNodeCount(){
		if(NODE_COUNT != -1) return NODE_COUNT;
		else{
			InputStream inStream =null;
			try{
		    	inStream = HareEnv.class.getResourceAsStream(INFO_FILE);
		    	Properties systemInfoProp = new Properties();
		    	systemInfoProp.load(inStream);
		    	String count = systemInfoProp.get("nodeCount").toString();
		    	NODE_COUNT = Integer.valueOf(count);
		    	return NODE_COUNT;
		    	
	    	}catch(Exception e){
	    		throw new RuntimeException(e);
	    	}finally{
	    		if(inStream !=null){
	    			try {
						inStream.close();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
	    		}
	    	}
		}
	}
	
	public static String getVerificationfilename() {
		return verificationFileName;
	}
   
}
