package com.haredb.client.rest.resource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.haredb.client.facade.until.HareSparkSysConfig;
import com.haredb.client.listener.WebRestAppListener;
import com.haredb.client.util.ConnectionUtil;
import com.haredb.client.util.ConnectionUtil.Mode;
import com.haredb.harespark.bean.input.AlterTableBean;
import com.haredb.harespark.bean.input.CreateTableBean;
import com.haredb.harespark.bean.input.DeleteDataFileBean;
import com.haredb.harespark.bean.input.DescribeTableBean;
import com.haredb.harespark.bean.input.DropTableBean;
import com.haredb.harespark.bean.input.PreviewBean;
import com.haredb.harespark.bean.input.QueryStatusBean;
import com.haredb.harespark.bean.input.QuerySubmitBean;
import com.haredb.harespark.bean.input.UploadDataFileBean;
import com.haredb.harespark.bean.input.UploadDataFileStatusBean;
import com.haredb.harespark.bean.input.UserSessionBean;
import com.haredb.harespark.bean.response.DescribeTableResponseBean;
import com.haredb.harespark.bean.response.PreviewResponseBean;
import com.haredb.harespark.bean.response.QueryStatusResponseBean;
import com.haredb.harespark.bean.response.QuerySubmitResponseBean;
import com.haredb.harespark.bean.response.ResponseInfoBean;
import com.haredb.harespark.bean.response.UploadDataFileResponseBean;
import com.haredb.harespark.bean.response.UploadDataFileStatusResponseBean;
import com.haredb.harespark.common.SysConfig;
import com.haredb.harespark.operator.HareSparkOperator;

@Path("harespark")
public class HareSparkResource {
	
	
	
	@POST
	@Path("usersession")
	@Produces(MediaType.APPLICATION_JSON)
	public ResponseInfoBean createUserSession(@Context HttpServletRequest request, UserSessionBean userSessionBean){
		ResponseInfoBean bean = null;		
		try {
			
			
			HareSparkOperator operator = new HareSparkOperator();
			bean = operator.createUserSession(userSessionBean);
			ConnectionUtil util = new ConnectionUtil();		
			bean.setConnectionKey(util.genConnectionKey(10, Mode.ALPHANUMERIC));
			if(bean.getStatus().equals(bean.SUCCESS)) {
				request.getSession().setAttribute(userSessionBean.sessionKey, userSessionBean);
			}			
		} catch (Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException(e.getMessage());			
			e.printStackTrace();
		}		
		return bean;
	}
	
	
	@POST
	@Path("createtable")
	@Produces(MediaType.APPLICATION_JSON)
	public ResponseInfoBean createTable(@Context HttpServletRequest request, CreateTableBean createTableBean){
		ResponseInfoBean bean = null;		
		try {
			HareSparkSysConfig hareSparkSysConfig = (HareSparkSysConfig)request.getServletContext().getAttribute(WebRestAppListener.HARESPARKCONFIGSTR);
			
			UserSessionBean userSessionBean = (UserSessionBean) request.getSession().getAttribute(UserSessionBean.sessionKey);
			HareSparkOperator operator = new HareSparkOperator(userSessionBean, hareSparkSysConfig);
			bean = operator.createTable(createTableBean);
		} catch (Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException(e.getMessage());			
			e.printStackTrace();
		}		
		return bean;
		
	}
	
	@POST
	@Path("uploaddatafile")
	@Produces(MediaType.APPLICATION_JSON)
	public UploadDataFileResponseBean uploadDataFile(@Context HttpServletRequest request, UploadDataFileBean uploadDataFileBean){
		UploadDataFileResponseBean bean = null;		
		try {
			HareSparkSysConfig hareSparkSysConfig = (HareSparkSysConfig)request.getServletContext().getAttribute(WebRestAppListener.HARESPARKCONFIGSTR);
			
			UserSessionBean userSessionBean = (UserSessionBean) request.getSession().getAttribute(UserSessionBean.sessionKey);
			HareSparkOperator operator = new HareSparkOperator(userSessionBean, hareSparkSysConfig);
			bean = operator.uploadDataFile(uploadDataFileBean);
		} catch (Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException(e.getMessage());			
			e.printStackTrace();
		}		
		return bean;
		
	}
	
	@POST
	@Path("uploaddatafile/status")
	@Produces(MediaType.APPLICATION_JSON)
	public UploadDataFileStatusResponseBean uploadDataFileStatus(@Context HttpServletRequest request, UploadDataFileStatusBean uploadDataFileStatusBean){
		UploadDataFileStatusResponseBean bean = null;		
		try {
			HareSparkSysConfig hareSparkSysConfig = (HareSparkSysConfig)request.getServletContext().getAttribute(WebRestAppListener.HARESPARKCONFIGSTR);
			
			UserSessionBean userSessionBean = (UserSessionBean) request.getSession().getAttribute(UserSessionBean.sessionKey);
			HareSparkOperator operator = new HareSparkOperator(userSessionBean, hareSparkSysConfig);
			bean = operator.uploadDataFileStatus(uploadDataFileStatusBean);
		} catch (Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException(e.getMessage());			
			e.printStackTrace();
		}		
		return bean;
		
	}
	
	@POST
	@Path("querysubmit")
	@Produces(MediaType.APPLICATION_JSON)
	public QuerySubmitResponseBean querySubmit(@Context HttpServletRequest request, QuerySubmitBean querySubmitBean){
		QuerySubmitResponseBean bean = null;		
		try {
			HareSparkSysConfig hareSparkSysConfig = (HareSparkSysConfig)request.getServletContext().getAttribute(WebRestAppListener.HARESPARKCONFIGSTR);
			
			UserSessionBean userSessionBean = (UserSessionBean) request.getSession().getAttribute(UserSessionBean.sessionKey);
			HareSparkOperator operator = new HareSparkOperator(userSessionBean, hareSparkSysConfig);
			bean = operator.querySubmit(querySubmitBean);
		} catch (Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException(e.getMessage());			
			e.printStackTrace();
		}		
		return bean;
		
	}
	
	@POST
	@Path("querystatus")
	@Produces(MediaType.APPLICATION_JSON)
	public QueryStatusResponseBean queryStatus(@Context HttpServletRequest request, QueryStatusBean queryStatusBean){
		QueryStatusResponseBean bean = null;		
		try {
			HareSparkSysConfig hareSparkSysConfig = (HareSparkSysConfig)request.getServletContext().getAttribute(WebRestAppListener.HARESPARKCONFIGSTR);
			
			UserSessionBean userSessionBean = (UserSessionBean) request.getSession().getAttribute(UserSessionBean.sessionKey);
			HareSparkOperator operator = new HareSparkOperator(userSessionBean, hareSparkSysConfig);
			bean = operator.queryStatus(queryStatusBean);
		} catch (Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException(e.getMessage());			
			e.printStackTrace();
		}		
		return bean;
		
	}
	
	
	
	@POST
	@Path("preview")
	@Produces(MediaType.APPLICATION_JSON)
	public PreviewResponseBean preview(@Context HttpServletRequest request, PreviewBean previewBean){
		PreviewResponseBean bean = null;		
		try {
			HareSparkSysConfig hareSparkSysConfig = (HareSparkSysConfig)request.getServletContext().getAttribute(WebRestAppListener.HARESPARKCONFIGSTR);
			
			UserSessionBean userSessionBean = (UserSessionBean) request.getSession().getAttribute(UserSessionBean.sessionKey);
			HareSparkOperator operator = new HareSparkOperator(userSessionBean, hareSparkSysConfig);
			bean = operator.preview(previewBean);
		} catch (Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException(e.getMessage());			
			e.printStackTrace();
		}		
		return bean;
		
	}
	
	@POST
	@Path("deletedatafile")
	@Produces(MediaType.APPLICATION_JSON)
	public ResponseInfoBean deletedatafile(@Context HttpServletRequest request, DeleteDataFileBean deleteDataFileBean){
		ResponseInfoBean bean = null;		
		try {
			HareSparkSysConfig hareSparkSysConfig = (HareSparkSysConfig)request.getServletContext().getAttribute(WebRestAppListener.HARESPARKCONFIGSTR);
			
			UserSessionBean userSessionBean = (UserSessionBean) request.getSession().getAttribute(UserSessionBean.sessionKey);
			HareSparkOperator operator = new HareSparkOperator(userSessionBean, hareSparkSysConfig);
			bean = operator.deleteDataFile(deleteDataFileBean);
		} catch (Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException(e.getMessage());			
			e.printStackTrace();
		}		
		return bean;
		
	}
	
	
	@POST
	@Path("droptable")
	@Produces(MediaType.APPLICATION_JSON)
	public ResponseInfoBean dropTable(@Context HttpServletRequest request, DropTableBean dropTableBean){
		ResponseInfoBean bean = null;		
		try {
			HareSparkSysConfig hareSparkSysConfig = (HareSparkSysConfig)request.getServletContext().getAttribute(WebRestAppListener.HARESPARKCONFIGSTR);
			
			UserSessionBean userSessionBean = (UserSessionBean) request.getSession().getAttribute(UserSessionBean.sessionKey);
			HareSparkOperator operator = new HareSparkOperator(userSessionBean, hareSparkSysConfig);
			bean = operator.dropTable(dropTableBean);
		} catch (Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException(e.getMessage());			
			e.printStackTrace();
		}		
		return bean;
		
	}
	
	@POST
	@Path("altertable")
	@Produces(MediaType.APPLICATION_JSON)
	public ResponseInfoBean altertable(@Context HttpServletRequest request, AlterTableBean alterTableBean){
		ResponseInfoBean bean = null;		
		try {
			HareSparkSysConfig hareSparkSysConfig = (HareSparkSysConfig)request.getServletContext().getAttribute(WebRestAppListener.HARESPARKCONFIGSTR);
			
			UserSessionBean userSessionBean = (UserSessionBean) request.getSession().getAttribute(UserSessionBean.sessionKey);
			HareSparkOperator operator = new HareSparkOperator(userSessionBean, hareSparkSysConfig);
			bean = operator.alterTable(alterTableBean);
		} catch (Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException(e.getMessage());			
			e.printStackTrace();
		}		
		return bean;
		
	}
	
	@POST
	@Path("describetable")
	@Produces(MediaType.APPLICATION_JSON)
	public DescribeTableResponseBean describetable(@Context HttpServletRequest request, DescribeTableBean describeTableBean){
		DescribeTableResponseBean bean = null;		
		try {
			HareSparkSysConfig hareSparkSysConfig = (HareSparkSysConfig)request.getServletContext().getAttribute(WebRestAppListener.HARESPARKCONFIGSTR);
			
			UserSessionBean userSessionBean = (UserSessionBean) request.getSession().getAttribute(UserSessionBean.sessionKey);
			HareSparkOperator operator = new HareSparkOperator(userSessionBean, hareSparkSysConfig);
			bean = operator.describeTable(describeTableBean);
		} catch (Exception e) {
			bean.setStatus(bean.ERROR);
			bean.setException(e.getMessage());			
			e.printStackTrace();
		}		
		return bean;
	}
	
	@POST
	@Path("isExists")
	@Produces(MediaType.APPLICATION_JSON)
	public String isExists(@Context HttpServletRequest request, String tableName){
		HareSparkSysConfig hareSparkSysConfig = (HareSparkSysConfig)request.getServletContext().getAttribute(WebRestAppListener.HARESPARKCONFIGSTR);
		
		UserSessionBean userSessionBean = (UserSessionBean) request.getSession().getAttribute(UserSessionBean.sessionKey);
		HareSparkOperator operator = new HareSparkOperator(userSessionBean, hareSparkSysConfig);
		return String.valueOf(operator.isExists(tableName));
	}
	
}
