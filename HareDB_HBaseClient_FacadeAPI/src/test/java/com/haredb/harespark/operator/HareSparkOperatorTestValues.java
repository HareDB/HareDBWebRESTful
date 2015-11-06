package com.haredb.harespark.operator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.haredb.harespark.bean.input.UserSessionBean;

public class HareSparkOperatorTestValues {

	public HareSparkOperatorTestValues() {
		
	}
	
	private String hdfsSamplefilepath = "/tmp/testfile/";
	private String hdfsResultFolderpath = "/tmp/";
	
	/*
	 * initial test values
	 */
	
	public String getTableName() {
		return "HareSparkIT1";
	}
	
	
	public UserSessionBean getUserSessionBean() {
		UserSessionBean bean = new UserSessionBean();
		ClassLoader classLoader = getClass().getClassLoader();	
		File file = new File(classLoader.getResource("config").getFile());	
		
		bean.setConfigurationFolderPath(file.getAbsolutePath());
		return bean;
	}
	public List<String> getOriginCols() {
		List<String> cols = new ArrayList<String>();		
		cols.add("id");
		cols.add("name");
		cols.add("type");	
		return cols;		
	}
	public List<String> getAlterCols() {
		List<String> cols = new ArrayList<String>();		
		cols.add("id");
		cols.add("name");
		cols.add("location");	
		return cols;		
	}
	public List<String> getOriginDataType() {
		List<String> datatypes = new ArrayList<String>();
		datatypes.add("String");
		datatypes.add("String");
		datatypes.add("String");
		return datatypes;
	}
	
	public List<String> getAlterDataType() {
		List<String> datatypes = new ArrayList<String>();
		datatypes.add("Integer");
		datatypes.add("String");
		datatypes.add("String");
		return datatypes;
	}
	
	public void uploadSampleDataFile() {
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource("config").getFile());
		
		Configuration config = new Configuration();
		config.addResource(new Path(file.getPath() + "/core-site.xml"));
		config.addResource(new Path(file.getPath() + "/yarn-site.xml"));		
		
		file = new File(classLoader.getResource("testfile").getFile());
		
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(config);
			hdfs.copyFromLocalFile(new Path(file.getPath()), new Path(hdfsSamplefilepath));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
	}
	
	
	public void deleteSampleDataFile() {
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource("config").getFile());
		
		Configuration config = new Configuration();
		config.addResource(new Path(file.getPath() + "/core-site.xml"));
		config.addResource(new Path(file.getPath() + "/yarn-site.xml"));		
		
		
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(config);
			hdfs.delete(new Path(hdfsSamplefilepath), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
	}
	
	public void deleteSampleResultFolder() {
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource("config").getFile());
		
		Configuration config = new Configuration();
		config.addResource(new Path(file.getPath() + "/core-site.xml"));
		config.addResource(new Path(file.getPath() + "/yarn-site.xml"));		
		
		
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(config);
			hdfs.delete(new Path(getResultFolderPath1()), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		try {
			hdfs = FileSystem.get(config);
			hdfs.delete(new Path(getResultFolderPath2()), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
	}
	
	public void viewSQLResultData(String resultFolderPath) {
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource("config").getFile());
		
		Configuration config = new Configuration();
		config.addResource(new Path(file.getPath() + "/core-site.xml"));
		config.addResource(new Path(file.getPath() + "/yarn-site.xml"));		
		
		
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(config);
			BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(new Path(resultFolderPath))));
            String line;
            line=br.readLine();
            while (line != null){
                    System.out.println(line);
                    line=br.readLine();
            }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
	}
	
	
	public String getDataHadoopFile1() {
		return hdfsSamplefilepath + getDataFile1();
	}
	
	public String getDataHadoopFile2() {
		return hdfsSamplefilepath + getDataFile2();
	}
	
	public String getDataFile1() {		
		return "fruit1.csv";
	}
	
	public String getDataFile2() {
		return "fruit2.csv";
	}
	
	public String getResultFolderPath1() {
		return hdfsResultFolderpath + "test1/";		
	}
	
	public String getResultFolderPath2() {
		return hdfsResultFolderpath + "test2/";		
	}
	
	public String getSQL() {
		return "select * from " + getTableName();
	}
	
}
