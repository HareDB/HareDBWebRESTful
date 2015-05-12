package com.haredb.client.util;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import com.haredb.client.facade.bean.ConnectionBean;
import com.haredb.client.facade.bean.MessageInfo;
import com.haredb.hbaseclient.core.Connection;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumHiveMetaStoreConnectType;

public class ConnectionUtil {
	public static String connectionSessionName = "connection";
	public static String hiveMetaDataSessionName = "hiveMetaConnection";
	public static Properties prop = new Properties();
	
	private Connection conn;
	private HiveMetaConnectionBean metaConn;
	private ConnectionBean connBean;
	private List<String> checkItem = null;
	
	/**
	 * @return the metaConn
	 */
	public HiveMetaConnectionBean getMetaConn() {
		return metaConn;
	}


	/**
	 * @return the conn
	 */
	public Connection getConn() {
		return conn;
	}


	public ConnectionUtil(){
		//do nothing
	}
	
	
	public ConnectionUtil(ConnectionBean connBean){
		this.connBean = connBean;
		setSetcheckItem();
	}
	
	/**
	 * Resolve ConnectionBean
	 * @return
	 */
	public MessageInfo resolveBean() {
		MessageInfo info = checkProperty();
		if(info.getStatus().equals(MessageInfo.SUCCESS) == false){
			return info;
		}
		try {
			prop.load(ConnectionUtil.class.getResourceAsStream("/connection.property"));
		
			this.conn = new Connection(connBean.getZookeeperHost(), connBean.getZookeeperPort());
			this.conn.setNameNodeHostPort(connBean.getNameNodeHostPort());
			this.conn.setRmAddressHostPort(connBean.getRmAddressHostPort());
			this.conn.setRmSchedulerAddressHostPort(connBean.getRmSchedulerAddressHostPort());
			this.conn.setRmResourceTrackerAddressHostPort(connBean.getRmResourceTrackerAddressHostPort());
			this.conn.setRmAdminAddressHostPort(connBean.getRmAdminAddressHostPort());
			this.conn.setMrJobhistoryAddress(connBean.getMrJobhistoryAddress());
			this.conn.setYarnNodeManagerAuxServices(prop.getProperty("yarnNodeManagerAuxServices"));
			this.conn.create();
			
			if(prop.getProperty("platform").equals("cloudera")){
				this.conn.setYarnApplicationClasspath(prop.getProperty("yarnApplicationClasspath"));
			}
			
			this.metaConn = new HiveMetaConnectionBean();
			EnumHiveMetaStoreConnectType connectionTypeEnum = null;
			if(connBean.getHiveConnType().toString().equals(EnumHiveMetaStoreConnectType.LOCAL.toString())){
				connectionTypeEnum = EnumHiveMetaStoreConnectType.LOCAL;
				this.metaConn.setMetaStoreConnectDriver(connBean.getMetaStoreConnectDriver());
				this.metaConn.setMetaStoreConnectURL(connBean.getMetaStoreConnectURL());
				this.metaConn.setMetaStoreConnectUserName(connBean.getMetaStoreConnectUserName());
				this.metaConn.setMetaStoreConnectPassword(connBean.getMetaStoreConnectPassword());
			}else if(connBean.getHiveConnType().toString().equals(EnumHiveMetaStoreConnectType.REMOTE.toString())){
				this.metaConn.setMetaUris(connBean.getMetaUris());
				connectionTypeEnum = EnumHiveMetaStoreConnectType.REMOTE;
			}else{
				this.metaConn.setEmbedPath(connBean.getEmbedPath());
				connectionTypeEnum = EnumHiveMetaStoreConnectType.EMBED;
			}
			this.metaConn.setHiveConnType(connectionTypeEnum);
			
			//set connection key
			info.setConnectionKey(genConnectionKey(10,Mode.ALPHANUMERIC));
			
		} catch (Exception e) {
			info.setStatus(MessageInfo.ERROR);
			info.setException(e.getMessage());
		}
		return info;
		
	}
	
	
	public Connection getConnection(HttpServletRequest request) {
		try {
			Connection connection = (Connection)request.getSession().getAttribute(connectionSessionName);
			if(connection == null) {
				/* hard code for connect
				throw new Exception("Connection is null, please reconnect!");
				*/
				Properties prop = new Properties();
				prop.load(ConnectionUtil.class.getResourceAsStream("/connection.property"));
				String zookeeperHostName = prop.getProperty("zookeeperHostName");
				String zookeeperPort = prop.getProperty("zookeeperHostPort");
				String nameNodeHostPort = prop.getProperty("nameNodeHostPort");
				connection = new Connection(zookeeperHostName, zookeeperPort);
				connection.setNameNodeHostPort(nameNodeHostPort);
				connection.setRmAddressHostPort(prop.getProperty("rmAddressHostPort"));
				connection.setRmSchedulerAddressHostPort(prop.getProperty("rmSchedulerAddressHostPort"));
				connection.setRmResourceTrackerAddressHostPort(prop.getProperty("rmResourceTrackerAddressHostPort"));
				connection.setRmAdminAddressHostPort(prop.getProperty("rmAdminAddressHostPort"));
				connection.setMrJobhistoryAddress(prop.getProperty("mrJobhistoryAddress"));
				connection.setYarnNodeManagerAuxServices("mapreduce_shuffle");
				connection.setYarnApplicationClasspath(prop.getProperty("yarnApplicationClasspath"));
				connection.create();
				
			}
			return connection;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		
	}
	
	public HiveMetaConnectionBean getHiveMetaDataConnection(HttpServletRequest request){		
		try {
			HiveMetaConnectionBean hiveMetaConnectionBean = (HiveMetaConnectionBean)request.getSession().getAttribute(hiveMetaDataSessionName);
			if(hiveMetaConnectionBean == null) {
				/* hard code for connect
				throw new Exception("Connection is null, please reconnect!");
				*/
				Properties prop = new Properties();
				prop.load(ConnectionUtil.class.getResourceAsStream("/hiveMetaDataConnection.property"));
				
				hiveMetaConnectionBean = new HiveMetaConnectionBean();
				String connectionType = prop.getProperty("connectionType");
				EnumHiveMetaStoreConnectType connectionTypeEnum = null;
				if(connectionType.equals("LOCAL")){
					connectionTypeEnum = EnumHiveMetaStoreConnectType.LOCAL;
					hiveMetaConnectionBean.setHiveConnType(connectionTypeEnum);
					hiveMetaConnectionBean.setMetaStoreConnectDriver(prop.getProperty("connectionDriver"));
					hiveMetaConnectionBean.setMetaStoreConnectURL(prop.getProperty("connectionURL"));
					hiveMetaConnectionBean.setMetaStoreConnectUserName(prop.getProperty("connectionUserName"));
					hiveMetaConnectionBean.setMetaStoreConnectPassword(prop.getProperty("connectionPassword"));
				
				}else if(connectionType.equals("REMOTE")){
					connectionTypeEnum = EnumHiveMetaStoreConnectType.REMOTE;
				}else{
					connectionTypeEnum = EnumHiveMetaStoreConnectType.EMBED;
				}
			}
			return hiveMetaConnectionBean;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
	private enum Mode {
	    ALPHA, ALPHANUMERIC, NUMERIC 
	}
	
	private String genConnectionKey(int length, Mode mode) throws Exception {

		StringBuffer buffer = new StringBuffer();
		String characters = "";

		switch(mode){
		
		case ALPHA:
			characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
			break;
		
		case ALPHANUMERIC:
			characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
			break;
	
		case NUMERIC:
			characters = "1234567890";
		    break;
		}
		
		int charactersLength = characters.length();

		for (int i = 0; i < length; i++) {
			double index = Math.random() * charactersLength;
			buffer.append(characters.charAt((int) index));
		}
		return buffer.toString();
	}
	
	private MessageInfo checkProperty(){
		MessageInfo info = new MessageInfo();
		try {
			String propertyName = null;
			Object value = null;
			BeanInfo beanInfo = Introspector.getBeanInfo(ConnectionBean.class);
			for (PropertyDescriptor propertyDesc : beanInfo.getPropertyDescriptors()) {
			    propertyName = propertyDesc.getName();
			    if(checkItem.contains(propertyName)) {
			    	value = propertyDesc.getReadMethod().invoke(connBean);
				    if(value == null) {
				    	throw new Exception(propertyName+" is null!");
				    }
			    }
			    	
			    
			}
			info.setStatus(MessageInfo.SUCCESS);
		} catch (Exception e) {
			info.setStatus(MessageInfo.ERROR);
			info.setException(e.getMessage());
			return info;
		}
		
		return info;
	}
	
	private void setSetcheckItem() {
		if(checkItem == null) {
			checkItem = new ArrayList<String>();
			checkItem.add("zookeeperHost");
			checkItem.add("zookeeperPort");
			checkItem.add("nameNodeHostPort");
			checkItem.add("rmAddressHostPort");
			checkItem.add("rmSchedulerAddressHostPort");
			checkItem.add("rmResourceTrackerAddressHostPort");
			checkItem.add("rmAdminAddressHostPort");
			checkItem.add("mrJobhistoryAddress");
			
			checkItem.add("hiveConnType");
			checkItem.add("metaStoreConnectDriver");
			checkItem.add("metaStoreConnectURL");
			checkItem.add("metaStoreConnectUserName");
			checkItem.add("metaStoreConnectPassword");
		}
		
	}
	
}
