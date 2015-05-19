package com.haredb.client.facade.bean;

import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumHiveMetaStoreConnectType;
import com.haredb.hive.metastore.connection.HiveMetaConnectionBean.EnumSQLType;
@XmlRootElement
public class ConnectionBean {
    private String name;
    private String zookeeperHost;
    private String zookeeperPort;
    private String solrZKHosts;
	private Map<String, String> maps;
    private String jobTrackerHostPort;
    private String nameNodeHostPort;
    
    private String rmAddressHostPort;
    private String rmSchedulerAddressHostPort;
    private String rmResourceTrackerAddressHostPort;
    private String rmAdminAddressHostPort;
    private String mrJobhistoryAddress;
    private String rmMRFrameWorkName;
    private String yarnNodeManagerAuxServices;
    private String registerCoprocessorHdfsPath;
    
	private String embedPath;//embedded
	private EnumHiveMetaStoreConnectType hiveConnType;
	private EnumSQLType dbBrand;
	private String connectionName;
	private String dbName;
	private String metaStoreConnectJdbcPath;
	private String metaStoreConnectURL;
	private String metaStoreConnectDriver;
	private String metaStoreConnectUserName;
	private String metaStoreConnectPassword;
	private String metaUris;//remote
	private String hdfsMetaStoreDir;//jack
	
	//Kerberos Principal
	private boolean enableKerberos;
	private String hbaseMasterPrincipal;
	private String hbaseRegionServerPrincipal;
	private String dfsNameNodePrincipal;
	private String dfsDataNodePrincipal;
	private String hiveMetaStorePrincipal;
	private String yarnResourceMgrPrincipal;
	private String yarnNodeMgrPrincipal;
    
    
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getZookeeperHost() {
		return zookeeperHost;
	}
	public void setZookeeperHost(String zookeeperHost) {
		this.zookeeperHost = zookeeperHost;
	}
	public String getZookeeperPort() {
		return zookeeperPort;
	}
	public void setZookeeperPort(String zookeeperPort) {
		this.zookeeperPort = zookeeperPort;
	}
	public String getSolrZKHosts() {
	    return this.solrZKHosts;
	  }
	  public void setSolrZKHosts(String solrZKHosts) {
	    this.solrZKHosts = solrZKHosts;
	  }
	public Map<String, String> getMaps() {
		return maps;
	}
	public void setMaps(Map<String, String> maps) {
		this.maps = maps;
	}
	public String getJobTrackerHostPort() {
		return jobTrackerHostPort;
	}
	public void setJobTrackerHostPort(String jobTrackerHostPort) {
		this.jobTrackerHostPort = jobTrackerHostPort;
	}
	public String getNameNodeHostPort() {
		return nameNodeHostPort;
	}
	public void setNameNodeHostPort(String nameNodeHostPort) {
		this.nameNodeHostPort = nameNodeHostPort;
	}
	public String getRmAddressHostPort() {
		return rmAddressHostPort;
	}
	public void setRmAddressHostPort(String rmAddressHostPort) {
		this.rmAddressHostPort = rmAddressHostPort;
	}
	public String getRmSchedulerAddressHostPort() {
		return rmSchedulerAddressHostPort;
	}
	public void setRmSchedulerAddressHostPort(String rmSchedulerAddressHostPort) {
		this.rmSchedulerAddressHostPort = rmSchedulerAddressHostPort;
	}
	public String getRmResourceTrackerAddressHostPort() {
		return rmResourceTrackerAddressHostPort;
	}
	public void setRmResourceTrackerAddressHostPort(
			String rmResourceTrackerAddressHostPort) {
		this.rmResourceTrackerAddressHostPort = rmResourceTrackerAddressHostPort;
	}
	public String getRmAdminAddressHostPort() {
		return rmAdminAddressHostPort;
	}
	public void setRmAdminAddressHostPort(String rmAdminAddressHostPort) {
		this.rmAdminAddressHostPort = rmAdminAddressHostPort;
	}
	public String getMrJobhistoryAddress() {
		return mrJobhistoryAddress;
	}
	public void setMrJobhistoryAddress(String mrJobhistoryAddress) {
		this.mrJobhistoryAddress = mrJobhistoryAddress;
	}
	public String getRmMRFrameWorkName() {
		return rmMRFrameWorkName;
	}
	public void setRmMRFrameWorkName(String rmMRFrameWorkName) {
		this.rmMRFrameWorkName = rmMRFrameWorkName;
	}
	public String getYarnNodeManagerAuxServices() {
		return yarnNodeManagerAuxServices;
	}
	public void setYarnNodeManagerAuxServices(String yarnNodeManagerAuxServices) {
		this.yarnNodeManagerAuxServices = yarnNodeManagerAuxServices;
	}
	public String getRegisterCoprocessorHdfsPath() {
		return registerCoprocessorHdfsPath;
	}
	public void setRegisterCoprocessorHdfsPath(String registerCoprocessorHdfsPath) {
		this.registerCoprocessorHdfsPath = registerCoprocessorHdfsPath;
	}
	public String getEmbedPath() {
		return embedPath;
	}
	public void setEmbedPath(String embedPath) {
		this.embedPath = embedPath;
	}
	public EnumHiveMetaStoreConnectType getHiveConnType() {
		return hiveConnType;
	}
	public void setHiveConnType(EnumHiveMetaStoreConnectType hiveConnType) {
		this.hiveConnType = hiveConnType;
	}
	public EnumSQLType getDbBrand() {
		return dbBrand;
	}
	public void setDbBrand(EnumSQLType dbBrand) {
		this.dbBrand = dbBrand;
	}
	public String getConnectionName() {
		return connectionName;
	}
	public void setConnectionName(String connectionName) {
		this.connectionName = connectionName;
	}
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public String getMetaStoreConnectJdbcPath() {
		return metaStoreConnectJdbcPath;
	}
	public void setMetaStoreConnectJdbcPath(String metaStoreConnectJdbcPath) {
		this.metaStoreConnectJdbcPath = metaStoreConnectJdbcPath;
	}
	public String getMetaStoreConnectURL() {
		return metaStoreConnectURL;
	}
	public void setMetaStoreConnectURL(String metaStoreConnectURL) {
		this.metaStoreConnectURL = metaStoreConnectURL;
	}
	public String getMetaStoreConnectDriver() {
		return metaStoreConnectDriver;
	}
	public void setMetaStoreConnectDriver(String metaStoreConnectDriver) {
		this.metaStoreConnectDriver = metaStoreConnectDriver;
	}
	public String getMetaStoreConnectUserName() {
		return metaStoreConnectUserName;
	}
	public void setMetaStoreConnectUserName(String metaStoreConnectUserName) {
		this.metaStoreConnectUserName = metaStoreConnectUserName;
	}
	public String getMetaStoreConnectPassword() {
		return metaStoreConnectPassword;
	}
	public void setMetaStoreConnectPassword(String metaStoreConnectPassword) {
		this.metaStoreConnectPassword = metaStoreConnectPassword;
	}
	public String getMetaUris() {
		return metaUris;
	}
	public void setMetaUris(String metaUris) {
		this.metaUris = metaUris;
	}
	public String getHdfsMetaStoreDir() {
		return hdfsMetaStoreDir;
	}
	public void setHdfsMetaStoreDir(String hdfsMetaStoreDir) {
		this.hdfsMetaStoreDir = hdfsMetaStoreDir;
	}
    
    
	public boolean isEnableKerberos() {
		return enableKerberos;
	}
	public void setEnableKerberos(boolean enableKerberos) {
		this.enableKerberos = enableKerberos;
	}
	public String getHbaseMasterPrincipal() {
		return hbaseMasterPrincipal;
	}
	public void setHbaseMasterPrincipal(String hbaseMasterPrincipal) {
		this.hbaseMasterPrincipal = hbaseMasterPrincipal;
	}
	public String getHbaseRegionServerPrincipal() {
		return hbaseRegionServerPrincipal;
	}
	public void setHbaseRegionServerPrincipal(String hbaseRegionServerPrincipal) {
		this.hbaseRegionServerPrincipal = hbaseRegionServerPrincipal;
	}
	public String getDfsNameNodePrincipal() {
		return dfsNameNodePrincipal;
	}
	public void setDfsNameNodePrincipal(String dfsNameNodePrincipal) {
		this.dfsNameNodePrincipal = dfsNameNodePrincipal;
	}
	public String getDfsDataNodePrincipal() {
		return dfsDataNodePrincipal;
	}
	public void setDfsDataNodePrincipal(String dfsDataNodePrincipal) {
		this.dfsDataNodePrincipal = dfsDataNodePrincipal;
	}
	public String getHiveMetaStorePrincipal() {
		return hiveMetaStorePrincipal;
	}
	public void setHiveMetaStorePrincipal(String hiveMetaStorePrincipal) {
		this.hiveMetaStorePrincipal = hiveMetaStorePrincipal;
	}
	public String getYarnResourceMgrPrincipal() {
		return yarnResourceMgrPrincipal;
	}
	public void setYarnResourceMgrPrincipal(String yarnResourceMgrPrincipal) {
		this.yarnResourceMgrPrincipal = yarnResourceMgrPrincipal;
	}
	public String getYarnNodeMgrPrincipal() {
		return yarnNodeMgrPrincipal;
	}
	public void setYarnNodeMgrPrincipal(String yarnNodeMgrPrincipal) {
		this.yarnNodeMgrPrincipal = yarnNodeMgrPrincipal;
	}
	
	
}
