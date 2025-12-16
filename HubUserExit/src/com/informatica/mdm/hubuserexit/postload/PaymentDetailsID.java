package com.informatica.mdm.hubuserexit.postload;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.json.XML;

public class PaymentDetailsID 
{

	public void PaymentDetailMethod(String rowIdobject, Statement stmt, String prtyRowIdobject, String tablename) 
	{
		Logger log = Logger.getLogger(PaymentDetailsID.class);
		
		{
			log.info("rowIdobject PaymentDetailsId - " + rowIdobject + "prtyRowIdobject - " + prtyRowIdobject + "tablename - " + tablename);
			ResultSet Locrs = null;
			ResultSet Pmntrs = null;
			
			try {
				
				Pmntrs = stmt.executeQuery("SELECT X_PMNT_DTLS_ID PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS WHERE HUB_STATE_IND = '1' AND ROWID_OBJECT ="
								+ rowIdobject + "");
								
				String PmntDtlsId = "";
				while (Pmntrs.next()) 
				{
					PmntDtlsId = Pmntrs.getString("PMNT_DTLS_ID");
					log.info("PMNT_DTLS_ID " + PmntDtlsId);
				}
				
				Locrs = stmt.executeQuery("SELECT X_PMNT_NM PMNT_NM FROM SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS WHERE HUB_STATE_IND = '1' AND X_PMNT_DTLS_ID ='"
								+ PmntDtlsId + "'");
				
				
				String PmntNm = "";
				while (Locrs.next()) 
				{
					PmntNm = Locrs.getString("PMNT_NM");
					log.info("PMNT_NM " + PmntNm);
				}
				
				//PmntNm = PmntNm.replace("&", "&amp;");
				//log.info("LocNm after replace - " + PmntNm);
				
				log.info("X_PMNT_DTLS_ID " + PmntDtlsId);
				log.info("X_PMNT_NM " + PmntNm);
				
				ResultSet newPmntrs = null;
				
				newPmntrs = stmt.executeQuery("SELECT X_PMNT_DTLS_ID PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XT_PMNT_LOC_NM WHERE HUB_STATE_IND = '1' AND X_PMNT_DTLS_ID ='"
								+ PmntDtlsId + "'");
				
				String newPmntDtlsId = null;
				while (newPmntrs.next()) 
				{
					newPmntDtlsId = newPmntrs.getString("PMNT_DTLS_ID");
					log.info("PMNT_DTLS_ID of C_XT_PMNT_LOC_NM  --> " + newPmntDtlsId);
				}
			
				if(newPmntDtlsId != null)
				{

					log.info("PMNT_LOC_NM of C_XO_PRTY_PMNT_DTLS  --> " + PmntNm);
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_LOC_NM SET X_PRTY_FK='"+ prtyRowIdobject + "', X_PMNT_LOC_NM ='"+ PmntNm +"'WHERE X_PMNT_DTLS_ID = '" + PmntDtlsId + "'");
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_LOC_NM_XREF SET X_PRTY_FK='"+ prtyRowIdobject + "',S_X_PRTY_FK='"+ prtyRowIdobject + "', X_PMNT_LOC_NM ='"+ PmntNm +"'WHERE X_PMNT_DTLS_ID = '" + PmntDtlsId + "'");
							log.info("X_PMNT_LOC_NM updated.");
					
				}
				else if (PmntNm != null && newPmntDtlsId == null)
				{
					PmntNm = PmntNm.replace("&", "&amp;");
					log.info("PmntNm after replace - " + PmntNm);
					
					Map<String, String> soapmap = soappmntdtlslov(PmntNm, PmntDtlsId, tablename);
							
					log.info("After soap call ");
							
					log.info("UPDATE SUPPLIER_HUB.C_XT_PMNT_LOC_NM"
							+" SET X_PRTY_FK='"+prtyRowIdobject+"'WHERE X_PMNT_DTLS_ID = '" + PmntDtlsId + "'");
							
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_LOC_NM"
							+" SET X_PRTY_FK='"+prtyRowIdobject+"'WHERE X_PMNT_DTLS_ID = '" + PmntDtlsId + "'");
							
					log.info("After REL update");
							
					log.info("UPDATE SUPPLIER_HUB.C_XT_PMNT_LOC_NM_XREF"
							+" SET X_PRTY_FK='"+prtyRowIdobject+"', S_X_PRTY_FK='"+prtyRowIdobject+"'WHERE X_PMNT_DTLS_ID = '" + PmntDtlsId + "'");
							
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_LOC_NM_XREF"
							+" SET X_PRTY_FK='"+prtyRowIdobject+"', S_X_PRTY_FK='"+prtyRowIdobject+"'WHERE X_PMNT_DTLS_ID = '" + PmntDtlsId + "'");
							
					log.info("After REL XREF update");
					log.info("Inserted in X_PMNT_LOC_NM ");
				}
				
				log.info("end PaymentDetailsID ");
							
				Locrs.close();
				Pmntrs.close();
			    }
			
				
			catch (Exception e) 
				{
				log.info("Exception in PaymentDetailsID" + e);
				}
		}
		
	}
	
	
	public Map<String, String> soappmntdtlslov(String pmntNm, String pmntDtlsId, String tablename) {
		Map<String, String> map = new HashMap();
		
		Logger log = Logger.getLogger(GeneratePrchsFrmId.class);
		
		  try {
		    log.info("Inside soappmntdtlslov ");
		   
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
			+ " </urn:recordKey>" + " <urn:record>"
			+ " <urn:field>"
			+ " <urn:stringValue>" + pmntDtlsId
			+ "</urn:stringValue>" + " <urn:name>X_PMNT_DTLS_ID</urn:name>" + " </urn:field>"
			+ " <urn:field>"
			+ " <urn:stringValue>" + pmntNm
			+ "</urn:stringValue>" + " <urn:name>X_PMNT_LOC_NM</urn:name>" + " </urn:field>"
			+ " <urn:siperianObjectUid>BASE_OBJECT.C_XT_PMNT_LOC_NM</urn:siperianObjectUid>"
			+ " </urn:record>" + " <urn:generateSourceKey>true</urn:generateSourceKey>" + " </urn:put>"
			+ " </soapenv:Body>" + " </soapenv:Envelope>";
		    // Create a StringEntity for the SOAP XML.
		    log.info("Inside soappmntdtlslov " + xml);
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
		    if (!tablename.equals("C_XT_PMNT_LOC_NM")) {

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

	public void PutCallForParentLookup(String XPRTY_FK, Statement stmt, Statement stmt2, Statement stmt3, String tablename, String XFULL_NM) {
		Logger log = Logger.getLogger(GeneratePrchsFrmId.class);

		  try {
		    if (XPRTY_FK != null) {
		      log.info("Update Nothing here");
		      Map < String, String > soapmap = new HashMap();
		      soapmap = soapPmntDetailsInsertAtParent(XPRTY_FK, tablename);
		      log.info("After soap call " + new Date());
		      String rID = "";
		      String rXrefID = "";
		      String sourceKeyID = "";
		      rID = soapmap.get("rowid");
		      rXrefID = soapmap.get("rowidXref");
		      sourceKeyID = soapmap.get("sourceKey");
		      log.info("After soap call " + rID);
		      log.info("After soap call " + rXrefID);
		      log.info("After soap call " + sourceKeyID);

		      stmt2.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PRTY_FK" +
		        " SET X_PRTY_FK='" + XPRTY_FK + "',X_SUPP_NM='" + XFULL_NM + "'" +
		        " WHERE ROWID_OBJECT=" + rID + "");
		      log.info("After REL update");

		      stmt3.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PRTY_FK_XREF" +
		        " SET X_PRTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF cbpx WHERE ROWID_OBJECT ='" + XPRTY_FK + "'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY ), S_X_PRTY_FK='" + XPRTY_FK + "',X_SUPP_NM='" + XFULL_NM + "'" +
		        " WHERE ROWID_XREF=" + rXrefID + "");
		      log.info("After REL XREF update");
		    }
		    log.info("end PutCallForParentLookupPaymentDetails ");
		  } catch (Exception e) {
		    log.info("Exception in PutCallForParentLookupPaymentDetails" + e);
		  }

		}

	public Map<String, String> soapPmntDetailsInsertAtParent(String XPRTY_FK, String tablename) {
		Map<String, String> map = new HashMap();
		Logger log = Logger.getLogger(GeneratePrchsFrmId.class);
		
		try {
			log.info("Inside soapDBAInsert ");
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
					+ " </urn:recordKey>" + " <urn:record>"
					+ " <urn:field>"
					+ " <urn:stringValue>" + XPRTY_FK 
					+ "</urn:stringValue>" + " <urn:name>X_PRTY_FK</urn:name>" + " </urn:field>"
					+ " <urn:siperianObjectUid>BASE_OBJECT.C_XT_PRTY_FK</urn:siperianObjectUid>"
					+ " </urn:record>" + " <urn:generateSourceKey>true</urn:generateSourceKey>" + " </urn:put>"
					+ " </soapenv:Body>" + " </soapenv:Envelope>";
			// Create a StringEntity for the SOAP XML.
			log.info("Inside soapDBAInsert "+xml);
			StringEntity stringEntity = new StringEntity(xml, "UTF-8");
			log.info("Inside soapDBAInsert1 ");
			stringEntity.setChunked(true);
			log.info("Inside soapDBAInsert2 ");
			// Request parameters and other properties.
			HttpPost httpPost = new HttpPost(
					EnvironmentVariable.env_url);
			log.info("Inside soapDBAInsert3 ");
			httpPost.setEntity(stringEntity);
			log.info("Inside soapDBAInsert4 ");
			httpPost.addHeader("Accept", "text/xml");
			log.info("Inside soapDBAInsert5 ");
			httpPost.addHeader("SOAPAction", "add");
			log.info("Inside soapDBAInsert6 ");
			// Execute and get the response.
			HttpClient httpClient = HttpClientBuilder.create().build();
			log.info("Inside soapDBAInsert7 ");
			// HttpClient httpClient = new DefaultHttpClient();
			HttpResponse response = httpClient.execute(httpPost);
			log.info("Inside soapDBAInsert8 ");
			String strResponse = null;
			if(!tablename.equals("C_XT_PRTY_FK"))
			{
				
				log.info("Inside soapDBAInsert9 ");
				HttpEntity entity = response.getEntity();
				log.info("Inside soapDBAInsert10 ");
				
				if (entity != null) {
					strResponse = EntityUtils.toString(entity);
				}
				log.info("SOAP Response " + strResponse);
			}
			log.info("Inside soapDBAInsert10 ");
		} catch (Exception e) {
			log.info("Exception in soapDBAInsert " + e);
		}
		return map;
	}
}