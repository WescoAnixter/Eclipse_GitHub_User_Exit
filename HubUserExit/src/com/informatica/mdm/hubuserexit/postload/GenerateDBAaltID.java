package com.informatica.mdm.hubuserexit.postload;

import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.log4j.Logger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.json.XML;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
//import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

public class GenerateDBAaltID {
	private static Logger log = Logger.getLogger(GenerateDBAaltID.class);

	public void GenerateUniqueDBAaltID(String rowIdobject, Statement stmt, String prtyRowIdobject) {
		
		ResultSet DBArs = null;
		ResultSet rs = null;
		try {
			
			log.info("RowidObject ----> " + rowIdobject);
			DBArs = stmt
					.executeQuery("select X_ALT_NM_ID AS DBA_NM_ID, X_ASSOC_DBA_NM AS ASSOC_DBA_NM from SUPPLIER_HUB.C_BO_PRTY_NM WHERE ROWID_OBJECT ='"
							+ rowIdobject + "'");
			String altDBANmId = "";
			String assocDBANm = "";
			while (DBArs.next()) {
				altDBANmId = DBArs.getString("DBA_NM_ID");
				log.info("X_ALT_NM_ID " + altDBANmId);
				assocDBANm = DBArs.getString("ASSOC_DBA_NM");
				log.info("ASSOC_DBA_NM " + assocDBANm);
			}		
			
			if(assocDBANm != null){
				rs = stmt.executeQuery("SELECT NM AS X_NM,PRTY_FK as PRTY_FK FROM SUPPLIER_HUB.C_BO_PRTY_NM WHERE X_ALT_NM_ID = '" + assocDBANm + "' AND PRTY_FK IS NOT NULL");
				String DBANm = "";
				String Prty_fk="";
				while (rs.next()) {
					DBANm = rs.getString("X_NM");
					Prty_fk = rs.getString("PRTY_FK");
					log.info("X_NM " + DBANm+"Prty_fk: "+Prty_fk );
				}
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM SET X_ALT_NM_ID = '" + assocDBANm
						+ "' , NM = '" + DBANm+ "',X_DBA_NM_PRTY_FK= '" +Prty_fk+ "' WHERE ROWID_OBJECT =" + rowIdobject + "");
				log.info("GenerateDBAaltID : Line 53");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM_XREF SET X_ALT_NM_ID = '" + assocDBANm
						+ "', NM = '" + DBANm + 
						"',X_DBA_NM_PRTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF WHERE ROWID_OBJECT ='"+Prty_fk+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY )"
						+",S_X_DBA_NM_PRTY_FK = '"+Prty_fk+"' WHERE ROWID_OBJECT =" + rowIdobject + "");
				log.info("GenerateDBAaltID : Line 58");
			}
			else{
				if (altDBANmId != null) {
					log.info("Update Nothing here");
				} else {
					rs = stmt.executeQuery("select MAX(X_ALT_NM_ID) AS MAX_DBA_NM_ID from SUPPLIER_HUB.C_BO_PRTY_NM");
					log.info("Conn334 " + rs);
					String updatedAltNmId = "";
					while (rs.next()) {
						String altNmId = rs.getString("MAX_DBA_NM_ID");
						if (altNmId != null) {
							log.info("MAX_DBA_NM_ID " + altNmId);

							int number = Integer.parseInt(altNmId.substring(4));
							number = number + 1;
							String numbers = Integer.toString(number);
							String initial = "SDBA";
							initial = initial.concat(numbers);
							updatedAltNmId = initial;
							log.info("X_ALT_NM_ID after increment " + updatedAltNmId);
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM SET X_ALT_NM_ID = '" + updatedAltNmId
									+ "' WHERE ROWID_OBJECT ='" + rowIdobject + "'");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM_XREF SET X_ALT_NM_ID = '" + updatedAltNmId
									+ "' WHERE ROWID_OBJECT ='" + rowIdobject + "'");
						} else {
							log.info("MAX_DBA_NM_ID is NULL");
							String numbers = "SDBA100001";
							log.info("MAX_DBA_NM_ID is " + numbers);
							updatedAltNmId = numbers;
							log.info("X_PRCHS_FRM_ID after increment " + updatedAltNmId);
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM SET X_ALT_NM_ID = '" + updatedAltNmId
									+ "' WHERE ROWID_OBJECT ='" + rowIdobject + "'");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM_XREF SET X_ALT_NM_ID = '" + updatedAltNmId
									+ "' WHERE ROWID_OBJECT ='" + rowIdobject + "'");
						}

						log.info("end GenerateUniqueWLEID ");

					}
					rs.close();
					DBArs.close();
				}
			}
			log.info("end GenerateUniqueDBAaltID ");
		} catch (Exception e) {
			log.info("Exception in GenerateUniqueDBAaltID" + e);
		}
	}
	
	public void PutCallForDBALookup(String rowIdobject, Statement stmt, Statement stmt2, Statement stmt3, String tablename) {
		
		ResultSet DBArs = null;
		//ResultSet DBAxref = null;
		ResultSet rs = null;
		ResultSet pfk = null;
		try {
			DBArs = stmt
					.executeQuery("SELECT X_ALT_NM_ID AS DBA_NM_ID, NM AS XNM , PRTY_FK AS XPRTY_FK from SUPPLIER_HUB.C_BO_PRTY_NM WHERE HUB_STATE_IND = 1 AND ROWID_OBJECT ='"
							+ rowIdobject + "'");
			String DBA_NM_ID = "";
			String XNM = "";
			String XPRTY_FK = "";
			String dba_prty_fk= "";
			while (DBArs.next()) {
				DBA_NM_ID = DBArs.getString("DBA_NM_ID");
				log.info("MAX_DBA_NM_ID " + DBA_NM_ID);
				XNM = DBArs.getString("XNM");
				log.info("XNM " + XNM);
				XPRTY_FK = DBArs.getString("XPRTY_FK");
				log.info("XPRTY_FK " + XPRTY_FK);
			}
			
			XNM = XNM.replace("&", "&amp;");
			log.info("XNM after replace - " +XNM);
			
			if (XPRTY_FK.isEmpty() || XPRTY_FK == null){
				pfk =  stmt.executeQuery("select X_DBA_NM_PRTY_FK AS DBA_PRTY_FK from SUPPLIER_HUB.C_BO_PRTY_NM WHERE HUB_STATE_IND = 1 AND ROWID_OBJECT ='"
							+ rowIdobject + "'");
				while (pfk.next()){
					dba_prty_fk = pfk.getString("DBA_PRTY_FK");
					log.info("X_DBA_NM_PRTY_FK: "+ dba_prty_fk);
					}
				XPRTY_FK = dba_prty_fk;
			}

			if (DBA_NM_ID != null) {
				log.info("Update Nothing here");
				Map<String, String> soapmap = new HashMap();
				soapmap=soapDBAInsert(DBA_NM_ID, XNM, XPRTY_FK, tablename);
				log.info("After soap call "+ new Date());
				/*String rID="";
				String rXrefID="";
				String sourceKeyID="";
				rID= soapmap.get("rowid");
				rXrefID=soapmap.get("rowidXref");
				sourceKeyID=soapmap.get("sourceKey");
				log.info("After soap call "+ rID);
				log.info("After soap call "+ rXrefID);
				log.info("After soap call "+ sourceKeyID);*/
				
				log.info("UPDATE SUPPLIER_HUB.C_XT_DDP_DBA_NM"
						+" SET X_PRTY_FK='"+XPRTY_FK+"'"
						+" WHERE X_PRTY_DBA_NM_ID='"+DBA_NM_ID+"'");
				
				stmt2.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_DDP_DBA_NM"
						+" SET X_PRTY_FK='"+XPRTY_FK+"'"
						+" WHERE X_PRTY_DBA_NM_ID='"+DBA_NM_ID+"'");
				
				
				log.info("Line 169: ");
				stmt3.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_DDP_DBA_NM_XREF"
						+" SET X_PRTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF cbpx WHERE ROWID_OBJECT ='"+XPRTY_FK+""
						+ "'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY ), S_X_PRTY_FK='"+XPRTY_FK+"'"
						+" WHERE X_PRTY_DBA_NM_ID='"+DBA_NM_ID+"'");
				
				log.info("Line 173");
				rs = stmt.executeQuery("select MAX(X_ALT_NM_ID) AS MAX_DBA_NM_ID from SUPPLIER_HUB.C_BO_PRTY_NM");
				log.info("Conn334 " + rs);
				rs.close();
				DBArs.close();
			}
			log.info("end GenerateUniqueDBAaltID ");
		} catch (Exception e) {
			log.info("Exception in GenerateUniqueDBAaltID" + e);
		}

	}

	public void PutCallForParentLookup(String XPRTY_FK, Statement stmt, Statement stmt2, Statement stmt3, String tablename, String XFULL_NM) {

		  try {
		    if (XPRTY_FK != null) {
		      log.info("Update Nothing here");
		      Map < String, String > soapmap = new HashMap();
		      soapmap = soapDBAInsertAtParent(XPRTY_FK, tablename);
		      log.info("After soap call " + new Date());
		      
		      log.info("XFULL_NM - " + XFULL_NM + " XPRTY_FK - " + XPRTY_FK);
		      /*String rID = "";
		      String rXrefID = "";
		      String sourceKeyID = "";
		      rID = soapmap.get("rowid");
		      rXrefID = soapmap.get("rowidXref");
		      sourceKeyID = soapmap.get("sourceKey");
		      log.info("After soap call " + rID);
		      log.info("After soap call " + rXrefID);
		      log.info("After soap call " + sourceKeyID);*/

		      stmt2.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PRTY_FK" +
		        " SET X_SUPP_NM='" + XFULL_NM + "'" +
		        " WHERE X_PRTY_FK='" + XPRTY_FK + "'");
		      log.info("After REL update");

		      stmt3.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PRTY_FK_XREF" +
		        " SET X_PRTY_FK='" + XPRTY_FK + "', S_X_PRTY_FK='" + XPRTY_FK + "',X_SUPP_NM='" + XFULL_NM + "'" +
		        " WHERE S_X_PRTY_FK='" + XPRTY_FK + "'");
		      log.info("After REL XREF update");
		    }
		    log.info("end GenerateUniqueDBAaltID ");
		  } catch (Exception e) {
		    log.info("Exception in GenerateUniqueDBAaltID" + e);
		  }

		}
	
	public Map<String, String> soapDBAInsert(String DBA_NM_ID, String XNM, String XPRTY_FK,String tablename) {
		Map<String, String> map = new HashMap();
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
					+ " <urn:stringValue>" + DBA_NM_ID
					+ "</urn:stringValue>" + " <urn:name>X_PRTY_DBA_NM_ID</urn:name>" + " </urn:field>"
					+ " <urn:field>"
					+ " <urn:stringValue>" + XNM
					+ "</urn:stringValue>" + " <urn:name>X_PRTY_DBA_NM</urn:name>" + " </urn:field>"
					+ " <urn:siperianObjectUid>BASE_OBJECT.C_XT_DDP_DBA_NM</urn:siperianObjectUid>"
					+ " </urn:record>" + " <urn:generateSourceKey>true</urn:generateSourceKey>" + " </urn:put>"
					+ " </soapenv:Body>" + " </soapenv:Envelope>";
			// Create a StringEntity for the SOAP XML.
			log.info("Inside soapDBAInsert "+xml);
			StringEntity stringEntity = new StringEntity(xml, "UTF-8");
			log.info("Inside soapDBAInsert1 ");
			stringEntity.setChunked(true);
			log.info("Inside soapDBAInsert2 ");
			// Request parameters and other properties.
			HttpPost httpPost = new HttpPost(EnvironmentVariable.env_url);
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
			if(!tablename.equals("C_XT_DDP_DBA_NM"))
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
	
	public Map<String, String> soapDBAInsertAtParent(String XPRTY_FK, String tablename) {
		Map<String, String> map = new HashMap();
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
