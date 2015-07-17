/**
 * 
 */
package com.haredb.client.facade.operator;

import static org.junit.Assert.*;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.haredb.client.facade.bean.IBean;
import com.haredb.hive.metastore.bean.DataType;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumSQLType;

/**
 * @author stana
 *
 */
public class HareMetaStoreOperatorTest {

	static String metaTableName = "unittest"; 
	static String hbaseTableName = "unittest"; 
	
	static HareMetaStoreOperator metaOperator;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		HiveMetaConnectionBean connectionBean = new HiveMetaConnectionBean(EnumSQLType.MYSQL, 
				"default", "jdbc:mysql://192.168.1.214:3306/hare", 
				"com.mysql.jdbc.Driver", "root", "123456");
		connectionBean.setHdfsMetaStoreDir("hdfs://host1:8020/user/hive/warehouse");
		metaOperator = new HareMetaStoreOperator(connectionBean);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		try {
			metaOperator.dropTable(metaTableName);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Test method for {@link com.haredb.client.facade.operator.HareMetaStoreOperator#createTable(java.lang.String, java.lang.String, java.util.List, java.util.List, java.util.List)}.
	 */
	@Test
	public void testCreateTable() {
		List<String> metaColumnNameList = new ArrayList<String>();
		List<String> hbaseColumnNameList = new ArrayList<String>();
		List<String> dataTypeList = new ArrayList<String>();
		
		metaColumnNameList.add("column1");
		metaColumnNameList.add("column2");
		metaColumnNameList.add("column3");
		
		hbaseColumnNameList.add(":key");
		hbaseColumnNameList.add("cf:col1");
		hbaseColumnNameList.add("cf:col2");
		
		dataTypeList.add(DataType.HIVE_DATATYPE_STRING);
		dataTypeList.add(DataType.HIVE_DATATYPE_STRING);
		dataTypeList.add(DataType.HIVE_DATATYPE_INT);
		
		try {
			System.out.println("create meta table:"+metaTableName+" ,mapping htable:"+hbaseTableName);
			metaOperator.createTable(metaTableName, hbaseTableName, metaColumnNameList, hbaseColumnNameList, dataTypeList);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Test method for {@link com.haredb.client.facade.operator.HareMetaStoreOperator#dropTable(java.lang.String)}.
	 */
	@Test
	public void testDropTable() {
	}

	/**
	 * Test method for {@link com.haredb.client.facade.operator.HareMetaStoreOperator#alterTable(java.lang.String, java.util.List, java.util.List, java.util.List)}.
	 */
	@Test
	public void testAlterTable_Datatype() {
		List<String> metaColumnNameList = new ArrayList<String>();
		List<String> hbaseColumnNameList = new ArrayList<String>();
		List<String> dataTypeList = new ArrayList<String>();
		
		metaColumnNameList.add("column1");
		metaColumnNameList.add("column2");
		metaColumnNameList.add("column3");
		
		hbaseColumnNameList.add(":key");
		hbaseColumnNameList.add("cf:col1");
		hbaseColumnNameList.add("cf:col2");
		
		dataTypeList.add(DataType.HIVE_DATATYPE_STRING);
		dataTypeList.add(DataType.HIVE_DATATYPE_STRING);
		dataTypeList.add(DataType.HIVE_DATATYPE_STRING);
		try {
			System.out.println("Alter meta table:"+metaTableName+" ,edit DataType");
			metaOperator.alterTable(metaTableName, metaColumnNameList, hbaseColumnNameList, dataTypeList);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Test method for {@link com.haredb.client.facade.operator.HareMetaStoreOperator#alterTable(java.lang.String, java.util.List, java.util.List, java.util.List)}.
	 */
	@Test
	public void testAlterTable_HTableName() {
		List<String> metaColumnNameList = new ArrayList<String>();
		List<String> hbaseColumnNameList = new ArrayList<String>();
		List<String> dataTypeList = new ArrayList<String>();
		
		metaColumnNameList.add("column1");
		metaColumnNameList.add("column2");
		metaColumnNameList.add("column3");
		
		hbaseColumnNameList.add(":key");
		hbaseColumnNameList.add("cf:name");
		hbaseColumnNameList.add("cf:id");
		
		dataTypeList.add(DataType.HIVE_DATATYPE_STRING);
		dataTypeList.add(DataType.HIVE_DATATYPE_STRING);
		dataTypeList.add(DataType.HIVE_DATATYPE_STRING);
		try {
			System.out.println("Alter meta table:"+metaTableName+" ,edit HTableName");
			metaOperator.alterTable(metaTableName, metaColumnNameList, hbaseColumnNameList, dataTypeList);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	
	@Test
	public void testAlterTable_MetaName() {
		List<String> metaColumnNameList = new ArrayList<String>();
		List<String> hbaseColumnNameList = new ArrayList<String>();
		List<String> dataTypeList = new ArrayList<String>();
		
		metaColumnNameList.add("column4");
		metaColumnNameList.add("column5");
		metaColumnNameList.add("column3");
		
		hbaseColumnNameList.add(":key");
		hbaseColumnNameList.add("cf:name");
		hbaseColumnNameList.add("cf:id");
		
		dataTypeList.add(DataType.HIVE_DATATYPE_STRING);
		dataTypeList.add(DataType.HIVE_DATATYPE_STRING);
		dataTypeList.add(DataType.HIVE_DATATYPE_STRING);
		try {
			System.out.println("Alter meta table:"+metaTableName+" ,edit MetaName");
			metaOperator.alterTable(metaTableName, metaColumnNameList, hbaseColumnNameList, dataTypeList);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	/**
	 * Test method for {@link com.haredb.client.facade.operator.HareMetaStoreOperator#describeTable(java.lang.String)}.
	 */
	@Test
	public void testDescribeTable() {
		try {
			IBean bean = metaOperator.describeTable(hbaseTableName);
			BeanInfo beanInfo = Introspector.getBeanInfo(bean.getBeanClass());
			String propertyName = null;
			Object value = null;
			System.out.println("Load Meta Table info:" + hbaseTableName);
			for (PropertyDescriptor propertyDesc : beanInfo.getPropertyDescriptors()) {
			    propertyName = propertyDesc.getName();
			    value = propertyDesc.getReadMethod().invoke(bean);
				if(value != null) {
				 	System.out.println("propertyName="+propertyName+", valeu="+value);
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Test method for {@link com.haredb.client.facade.operator.HareMetaStoreOperator#getAllTableNames()}.
	 */
	@Test
	public void testGetAllTableNames() {
		try {
			System.out.println("Get all Meta Table name:");
			List<String> list = metaOperator.getAllTableNames();
			for(String tb:list){
				System.out.println(tb);
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void test1(){
		
	}
	
}
