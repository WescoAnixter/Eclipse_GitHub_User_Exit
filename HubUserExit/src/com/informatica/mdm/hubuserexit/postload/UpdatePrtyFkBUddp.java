package com.informatica.mdm.hubuserexit.postload;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class UpdatePrtyFkBUddp 
{
	private static Logger log = Logger.getLogger(GeneratePrchsFrmId.class);
	
	public void UpdatePrtyFkBUddpNew(String rowIdobject,String prtyRowIdobject,Statement stmt, Statement stmt2) {
		
		log.info("We are in UpdatePrtyFkBUddpNew");
		
		log.info("RowidObject --- " +rowIdobject);
		log.info("PartyRowidObject --- " +prtyRowIdobject);
		
		try {
			if(rowIdobject != null && prtyRowIdobject != null) {
				
				log.info("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP SET X_PMNT_PRTY_FK='"+prtyRowIdobject
						+"' WHERE ROWID_OBJECT =" + rowIdobject + "");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP SET X_PMNT_PRTY_FK='"+prtyRowIdobject
						+"' WHERE ROWID_OBJECT =" + rowIdobject + "");
				log.info("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP_XREF SET S_X_PMNT_PRTY_FK='"+prtyRowIdobject
						+"',X_PMNT_PRTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF cbpx WHERE ROWID_OBJECT ='"
						+rowIdobject+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY ) WHERE ROWID_OBJECT =" + rowIdobject + "");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP_XREF SET S_X_PMNT_PRTY_FK='"+prtyRowIdobject
						+"',X_PMNT_PRTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF cbpx WHERE ROWID_OBJECT ='"
						+rowIdobject+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY ) WHERE ROWID_OBJECT =" + rowIdobject + "");
				}
			else {
				log.info("NULL VALUES FOUND.");
			}
			
		} catch (SQLException e) {
			log.info("Exception in UpdatePrtyFkBUddpNew. ---- " +e);
			e.printStackTrace();
		}
		
		
	}
	
	public void PreferredFlagDDP(String rowIdobject,String prtyRowIdobject,Statement stmt, String tablename) {
		
		log.info("We are in PreferredFlagDDP");
		
		log.info("RowidObject --- " +rowIdobject);
		log.info("PartyRowidObject --- " +prtyRowIdobject);
		
		try {
			if(rowIdobject != null && prtyRowIdobject != null) {
				
				ResultSet DDP = null;
				
				DDP = stmt.executeQuery("SELECT X_PRFRD_SUPP AS DDP_FLAG FROM SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP WHERE ROWID_OBJECT = '" + rowIdobject + "'");
				
				String ddpFlag = null;
				
				while (DDP.next()) {
					ddpFlag = DDP.getString("DDP_FLAG");
					log.info("DDP_FLAG value " + ddpFlag);
				}
				
				if(ddpFlag != null && ddpFlag.equals("Y")) {
					//soap call
					log.info("Updating from soap call");
					Map<String, String> soapmap = soapDDPpreferred(ddpFlag, tablename, prtyRowIdobject);
					log.info("DDP preffered flag checked");
					/*stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_DDP_PRFRD_SUPP = 'Y' WHERE ROWID_OBJECT =" + prtyRowIdobject + "");
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET X_DDP_PRFRD_SUPP = 'Y' WHERE ROWID_OBJECT =" + prtyRowIdobject + "");*/
				}
				else {
					log.info("The flag is either NULL or N");
				}
				
			}
			else {
				log.info("NULL VALUES FOUND.");
			}
			
		} catch (SQLException e) {
			log.info("Exception in PreferredFlagDDP ---- " +e);
			e.printStackTrace();
		}
		
		
	}

	
	public Map<String, String> soapDDPpreferred(String ddpFlag, String tablename, String prtyRowIdobject) {
		Map<String, String> map = new HashMap();
		
		Logger log = Logger.getLogger(UpdatePrtyFkBUddp.class);
		
		  try {
		    log.info("Inside soapDDPpreferred ");
		   
		    // String query_url =
		    // ""+ EnvironmentVariable.env_pswd +"https://wesco-devint.mdm.informaticahostednp.com:443/cmx/services/SifService";
		    String xml = "<?xml version=\"1.0\" encoding =\"utf-8\"?>" 
  		+ " <soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:urn=\"urn:siperian.api\">"
			+ " <soapenv:Header/>" + " <soapenv:Body>" + " <urn:put>" + " <!--Optional:-->"
			+ " <urn:username>admin</urn:username>" + " <!--Optional:-->" + " <urn:password>"
			+ " <urn:password>"+ EnvironmentVariable.env_pswd +"</urn:password>" + " <urn:encrypted>false</urn:encrypted>"
			+ " </urn:password>" + " <!--Optional:-->" + " <urn:orsId>orcl-SUPPLIER_HUB</urn:orsId>"
			+ " <urn:asynchronousOptions>"
			+ " <urn:isAsynchronous>false</urn:isAsynchronous>" + " </urn:asynchronousOptions>"
			+ " <urn:recordKey>" + " <!--Optional:-->" + " <urn:systemName>Admin</urn:systemName>"
			+ " <urn:rowid>" + 13927 + "</urn:rowid>"
			+ " </urn:recordKey>" + " <urn:record>"
			+ " <urn:field>"
			+ " <urn:stringValue>" + ddpFlag + "</urn:stringValue>" 
			+ " <urn:name>X_DDP_PRFRD_SUPP</urn:name>" + " </urn:field>"
			+ " <urn:siperianObjectUid>BASE_OBJECT.C_BO_PRTY</urn:siperianObjectUid>"
			+ " </urn:record>" + " <urn:generateSourceKey>false</urn:generateSourceKey>" + " </urn:put>"
			+ " </soapenv:Body>" + " </soapenv:Envelope>";
		    // Create a StringEntity for the SOAP XML.
		    log.info("Inside soappmntdtlslov " + xml);
		    log.info("XML - " + xml.replaceAll("\\s", ""));
		    StringEntity stringEntity = new StringEntity(xml, "UTF-8");
		    log.info("Inside soappmntdtlslov1 ");
		    stringEntity.setChunked(true);
		    log.info("Inside soappmntdtlslov2 ");
		    // Request parameters and other properties.
		    HttpPost httpPost = new HttpPost(EnvironmentVariable.env_url);
		    log.info("Inside soappmntdtlslov3 ");
		    httpPost.setEntity(stringEntity);
		    log.info("Inside soappmntdtlslov4 ");
		    httpPost.addHeader("Accept", "text/xml");
		    log.info("Inside soappmntdtlslov5 ");
		    httpPost.addHeader("SOAPAction", "add");
		    log.info("Inside soappmntdtlslov6 ");
		    // Execute and get the response.
		    HttpClient httpClient = HttpClientBuilder.create().build();
		    log.info("Inside soappmntdtlslov7 ");
		    // HttpClient httpClient = new DefaultHttpClient();
		    HttpResponse response = httpClient.execute(httpPost);
		    log.info("Inside soappmntdtlslov8 ");
		    String strResponse = null;
		    if (!tablename.equals("C_BO_PRTY")) {

		      log.info("Inside soappmntdtlslov9 ");
		      HttpEntity entity = response.getEntity();
		      log.info("Inside soappmntdtlslov10 ");

		      if (entity != null) {
		        strResponse = EntityUtils.toString(entity);
		      }
		      log.info("SOAP Response " + strResponse);
		    }
		    log.info("Inside soappmntdtlslov10 ");
		  } catch (Exception e) {
		    log.info("Exception in soappmntdtlslov " + e);
		  }
		  return map;
		
		}

	
}
