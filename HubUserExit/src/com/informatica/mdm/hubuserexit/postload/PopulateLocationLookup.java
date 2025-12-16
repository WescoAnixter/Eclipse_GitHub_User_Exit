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

	public class PopulateLocationLookup {
	private static Logger log = Logger.getLogger(GenerateDBAaltID.class);

	
	public void PutCallForLocLookup(String rowIdobject, String prtyFK, Statement stmt, Statement stmt2, Statement stmt3, String tablename) {
		
	ResultSet DBArs = null;
	try {
		
		log.info("Before C_BO_PSTL_ADDR Select ");
		String sql2 = "select X_ADDR_NM AS LocNm, X_PAY, HUB_STATE_IND from SUPPLIER_HUB.C_BO_PSTL_ADDR WHERE HUB_STATE_IND = '1' AND ROWID_OBJECT ="
				+ rowIdobject + "";
		log.info("Before C_BO_PSTL_ADDR Select "+sql2);
		DBArs = stmt.executeQuery("select X_ADDR_NM AS LocNm, X_PAY, HUB_STATE_IND from SUPPLIER_HUB.C_BO_PSTL_ADDR WHERE HUB_STATE_IND = '1' AND ROWID_OBJECT ="
						+ rowIdobject + "");
		String LocID = "";
		String LocNm = "";
		String xPay = "";
		String HubStateInd = null;
		while (DBArs.next()) {
			LocID = rowIdobject;
			log.info("LocID " + LocID);
			LocNm = DBArs.getString("LocNm");
			log.info("LocNm " + LocNm);
			xPay = DBArs.getString("X_PAY");
			log.info("X_PAY " + xPay);
			HubStateInd = DBArs.getString("HUB_STATE_IND");
			log.info("HUB_STATE_IND " + HubStateInd);
		}
		
		LocNm = LocNm.replace("&", "&amp;");
		log.info("LocNm after replace - " + LocNm);

		log.info("Condition Hub state - " + HubStateInd.equalsIgnoreCase("1"));
		
		if (LocID != null && HubStateInd.equalsIgnoreCase("1")) {
			log.info("LocId - " + LocID + "LocNm - " + LocNm + "PartyFK - " + prtyFK);
			
			Map<String, String> soapmap = new HashMap();
			soapmap=soapLocInsert(LocID, LocNm, xPay, prtyFK, tablename);
			
			log.info("After soap call "+ new Date());
			
			stmt2.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LOC_NM"
					+" SET X_PRTY_FK='"+prtyFK+"'"
					+" WHERE X_LOC_ID="+LocID+"");

			
			stmt3.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LOC_NM_XREF"
					+" SET X_PRTY_FK='"+prtyFK+"', S_X_PRTY_FK='"+prtyFK+"'"
					+" WHERE X_LOC_ID="+LocID+"");

			log.info("After REL XREF update");
			DBArs.close();
		}
		log.info("end PutCallForLocLookup ");
	} catch (Exception e) {
		log.info("Exception in PutCallForLocLookup" + e);
	}

	}

	public void PutCallForLocLookupUpdate(String rowIdobject, Statement stmt, Statement stmt2, Statement stmt3, String tablename, String PRTY_ID) {

	  ResultSet DBArs = null;
	  ResultSet DataTable = null;
	  ResultSet RXREF = null;
	  try {
		log.info("Getting ROWID_XREF from C_BO_PRTY :");
		String sql = "SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF WHERE ROWID_OBJECT = '" + PRTY_ID + "' ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY";
		log.info("SQL query - " + sql);
		RXREF = stmt2.executeQuery("SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF WHERE ROWID_OBJECT = '" + PRTY_ID + "' ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY");
		String rxref = "";
		while (RXREF.next()) {
			rxref = RXREF.getString("ROWID_XREF");
			log.info("ROWID_XREF of C_BO_PRTY - " + rxref);
		}
	
	    log.info("Before C_BO_PSTL_ADDR Select ");
	    String sql2 = "select X_ADDR_NM AS LocNm, X_PAY from SUPPLIER_HUB.C_BO_PSTL_ADDR WHERE ROWID_OBJECT =" +
	      rowIdobject + "";
	    log.info("Before C_BO_PSTL_ADDR Select " + sql2);
	    DBArs = stmt.executeQuery("select X_ADDR_NM AS LocNm, X_PAY from SUPPLIER_HUB.C_BO_PSTL_ADDR WHERE ROWID_OBJECT =" +
	        rowIdobject + "");
	    String LocID = "";
	    String LocNm = "";
	    String xPay = "";
	    while (DBArs.next()) {
	      LocID = rowIdobject;
	      log.info("LocID " + LocID);
	      LocNm = DBArs.getString("LocNm");
	      log.info("LocNm " + LocNm);
	      xPay = DBArs.getString("X_PAY");
	      log.info("X_PAY " + xPay);
	    }
	
	    LocNm = LocNm.replace("&", "&amp;");
	    log.info("LocNm after replace - " + LocNm);
	
	    if (LocID != null) {
	      log.info("Update Nothing here");
	
	      DataTable = stmt.executeQuery("SELECT ROWID_XREF, PKEY_SRC_OBJECT, ROWID_OBJECT from SUPPLIER_HUB.C_XT_LOC_NM_XREF WHERE X_LOC_ID ='" +
	        LocID + "' AND HUB_STATE_IND IN (0,1)");
	
	      String rowidXref = "";
	      String pkeySrcObject = "";
	      String rowid = "";
	
	      while (DataTable.next()) {
	        log.info("We are in While loop 2.");
	        rowidXref = DataTable.getString("ROWID_XREF");
	        log.info("ROWID_XREF -  " + rowidXref);
	        pkeySrcObject = DataTable.getString("PKEY_SRC_OBJECT");
	        log.info("ROWID_XREF -  " + pkeySrcObject);
	        rowid = DataTable.getString("ROWID_OBJECT");
	        log.info("ROWID -  " + rowid);
	      }
	      Map < String, String > map = new HashMap();
	      try {
	        log.info("Inside soapDBAUpdate ");
	        // String query_url =
	        // ""+ EnvironmentVariable.env_pswd +"https://wesco-devint.mdm.informaticahostednp.com:443/cmx/services/SifService";
	        String xml = "<?xml version=\"1.0\" encoding =\"utf-8\"?>" +
	          " <soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:urn=\"urn:siperian.api\">" +
	          " <soapenv:Header/>" + " <soapenv:Body>" + " <urn:put>" + " <!--Optional:-->" +
	          " <urn:username>admin</urn:username>" + " <!--Optional:-->" + " <urn:password>" +
	          " <urn:password>"+ EnvironmentVariable.env_pswd +"</urn:password>" + " <urn:encrypted>false</urn:encrypted>" +
	          " </urn:password>" + " <!--Optional:-->" + " <urn:orsId>orcl-SUPPLIER_HUB</urn:orsId>" +
	          " <urn:asynchronousOptions>" +
	          " <urn:isAsynchronous>false</urn:isAsynchronous>" + " </urn:asynchronousOptions>" +
	          " <urn:recordKey>" + " <!--Optional:-->" + " <urn:systemName>Admin</urn:systemName>" +
	          " <rowid>" + rowid + " </rowid>" +
	          " <sourceKey>" + pkeySrcObject + "</sourceKey>" +
	          " <rowidXref>" + rowidXref + "</rowidXref>" +
	          " </urn:recordKey>" + " <urn:record>" +
	          " <urn:field>" +
	          " <urn:stringValue>" + LocID +
	          "</urn:stringValue>" + " <urn:name>X_LOC_ID</urn:name>" + " </urn:field>" +
	          " <urn:field>" +
	          " <urn:stringValue>" + LocNm +
	          "</urn:stringValue>" + " <urn:name>X_LOC_NM</urn:name>" + " </urn:field>" +
	          " <urn:field>" +
	          " <urn:stringValue>" + xPay +
	          "</urn:stringValue>" + " <urn:name>X_PAY</urn:name>" + " </urn:field>" +
	          " <urn:field>" +
	          " <urn:stringValue>" + rxref +
	          "</urn:stringValue>" + " <urn:name>X_PRTY_FK</urn:name>" + " </urn:field>" +
	          " <urn:siperianObjectUid>BASE_OBJECT.C_XT_LOC_NM</urn:siperianObjectUid>" +
	          " </urn:record>" + " <urn:generateSourceKey>false</urn:generateSourceKey>" + " </urn:put>" +
	          " </soapenv:Body>" + " </soapenv:Envelope>";
	
	        // Create a StringEntity for the SOAP XML.
	        log.info("Inside soapDBAInsert " + xml);
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
	       
	        if (!tablename.equals("C_XT_LOC_NM")) {
	
	          log.info("Inside soapDBAInsert9 ");
	          HttpEntity entity = response.getEntity();
	          log.info("Inside soapDBAInsert10 ");
	
	          if (entity != null) {
	            strResponse = EntityUtils.toString(entity);
	          }
	          log.info("SOAP Response " + strResponse);
	        }
	      } catch (Exception e) {
	        log.info("Exception in locNm" + e);
	      }
	      DBArs.close();
	    }
	    log.info("end PutCallForLocLookupUpdate ");
	  } catch (Exception e) {
	    log.info("Exception in PutCallForLocLookupUpdate" + e);
	  }
	
	}


	public Map<String, String> soapLocInsert(String XLOCATION_ID, String XLOCATION_NAME, String XPAY, String XPRTY_FK,String tablename) {
		Map<String, String> map = new HashMap();
		try {
			log.info("Inside soapLocInsert ");
			// String query_url =
			// EnvironmentVariable.env_url;
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
					+ " <urn:stringValue>" + XLOCATION_ID
					+ "</urn:stringValue>" + " <urn:name>X_LOC_ID</urn:name>" + " </urn:field>"
					+ " <urn:field>"
					+ " <urn:stringValue>" + XLOCATION_NAME 
					+ "</urn:stringValue>" + " <urn:name>X_LOC_NM</urn:name>" + " </urn:field>"
					+ " <urn:field>"
					+ " <urn:stringValue>" + XPAY 
					+ "</urn:stringValue>" + " <urn:name>X_PAY</urn:name>" + " </urn:field>"
					+ " <urn:field>"
					+ " <urn:stringValue>" + XPRTY_FK
					+ "</urn:stringValue>" + " <urn:name>X_PRTY_FK</urn:name>" + " </urn:field>"
					+ " <urn:siperianObjectUid>BASE_OBJECT.C_XT_LOC_NM</urn:siperianObjectUid>"
					+ " </urn:record>" + " <urn:generateSourceKey>true</urn:generateSourceKey>" + " </urn:put>"
					+ " </soapenv:Body>" + " </soapenv:Envelope>";
			// Create a StringEntity for the SOAP XML.
			log.info("Inside soapLocInsert "+xml);
			StringEntity stringEntity = new StringEntity(xml, "UTF-8");
			log.info("Inside soapLocInsert1 ");
			stringEntity.setChunked(true);
			log.info("Inside soapLocInsert2 ");
			// Request parameters and other properties.
			HttpPost httpPost = new HttpPost(
					EnvironmentVariable.env_url);
			log.info("Inside soapLocInsert3 ");
			httpPost.setEntity(stringEntity);
			log.info("Inside soapLocInsert4 ");
			httpPost.addHeader("Accept", "text/xml");
			log.info("Inside soapLocInsert5 ");
			httpPost.addHeader("SOAPAction", "add");
			log.info("Inside soapLocInsert6 ");
			// Execute and get the response.
			HttpClient httpClient = HttpClientBuilder.create().build();
			log.info("Inside soapLocInsert7 ");
			// HttpClient httpClient = new DefaultHttpClient();
			HttpResponse response = httpClient.execute(httpPost);
			log.info("Inside soapLocInsert8 ");
			String strResponse = null;
			if(!tablename.equals("C_XT_LOC_NM"))
			{
				
				log.info("Inside soapLocInsert9 ");
				HttpEntity entity = response.getEntity();
				log.info("Inside soapLocInsert10 ");
				
				if (entity != null) {
					strResponse = EntityUtils.toString(entity);
				}
				log.info("SOAP Response " + strResponse);
			}
			log.info("Inside soapLocInsert10 ");
			
			/*JSONObject xmlJSONObj = XML.toJSONObject(strResponse);
			log.info("xmlresp2 " + xmlJSONObj);

			String jsonresp = xmlJSONObj.toString();
			log.info("xmlresp3 " + jsonresp);

			//JSONObject obj = new JSONObject(jsonresp);
			String rrecordKey="";
			String rrowidXref="";
			String ssourceKey="";
			JSONObject obj = new JSONObject(jsonresp);
			log.info("xmlresp4 ");
			JSONObject Envelope = obj.getJSONObject("soapenv:Envelope");
			log.info("xmlresp4 ");
			JSONObject Body = Envelope.getJSONObject("soapenv:Body");
			
			log.info("xmlresp4 ");
			JSONObject putReturn = Body.getJSONObject("putReturn");
			log.info("xmlresp4 ");
			JSONObject recordKey = putReturn.getJSONObject("recordKey");
			log.info("xmlresp4 ");
			//"sourceKey": 1260002040000,
			
			rrecordKey= Integer.toString(recordKey.getInt("rowid"));
			log.info("xmlresp4 ");
			rrowidXref= Integer.toString(recordKey.getInt("rowidXref"));
			log.info("xmlresp4 ");
			ssourceKey= Integer.toString(recordKey.getInt("sourceKey"));
			map.put("rowid", rrecordKey);
			map.put("rowidXref", rrowidXref);
			map.put("sourceKey", ssourceKey);*/
		} catch (Exception e) {
			log.info("Exception in soapLocInsert " + e);
		}
		return map;
	}
	
	public void PutCallForLookupBnkDtls(String rowIdobject, String prtyFK, Statement stmt, Statement stmt2, Statement stmt3, String tablename) {
		
		ResultSet DBArs = null;
		try {
			
			log.info("Before C_BO_PSTL_ADDR Select ");
			String sql2 = "select X_ADDR_NM AS LocNm from SUPPLIER_HUB.C_BO_PSTL_ADDR WHERE ROWID_OBJECT ="
					+ rowIdobject.trim() + "";
			log.info("Before C_BO_PSTL_ADDR Select "+sql2);
			DBArs = stmt
					.executeQuery("select X_ADDR_NM AS LocNm from SUPPLIER_HUB.C_BO_PSTL_ADDR WHERE ROWID_OBJECT ="
							+ rowIdobject.trim() + "");
			String LocID = "";
			String LocNm = "";
			while (DBArs.next()) {
				LocID = rowIdobject.trim();
				log.info("LocID " + LocID);
				LocNm = DBArs.getString("LocNm");
				log.info("LocNm " + LocNm);
			}
			
			LocNm = LocNm.replace("&", "&amp;");
			log.info("LocNm after replace - " + LocNm);

			if (LocID != null) {
				log.info("Update Nothing here");
				Map<String, String> soapmap = new HashMap();
					soapmap=soapLocInsertBnkDtls(LocID, LocNm, prtyFK, tablename);
					log.info("After soap call "+ new Date());
					
					stmt2.executeUpdate("UPDATE SUPPLIER_HUB.C_XR_BNK_ADDR_REL"
							+" SET X_PRTY_FK='"+prtyFK+"'"
							+" WHERE X_ASSOC_LOC_NM="+rowIdobject.trim()+"");
					log.info("After REL update");
					stmt3.executeUpdate("UPDATE SUPPLIER_HUB.C_XR_BNK_ADDR_REL_XREF"
							+" SET X_PRTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF cbpx WHERE ROWID_OBJECT ='"+prtyFK+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY), S_X_PRTY_FK='"+prtyFK+"'"
							+" WHERE X_ASSOC_LOC_NM="+rowIdobject.trim()+"");
					log.info("After REL XREF update");
			
				DBArs.close();
			}
			log.info("end PutCallForLookupBnkDtls ");
		} catch (Exception e) {
			log.info("Exception in PutCallForLookupBnkDtls" + e);
		}

	}

	public void PutCallForLookupBnkDtlsUpdate(String rowIdobject, Statement stmt, Statement stmt2, Statement stmt3, String tablename) {
		
		ResultSet DBArs = null;
		try {
			
			log.info("PutCallForLookupBnkDtlsUpdate rowIdobject"+rowIdobject+"tablename: "+tablename );
			String sql2 = "select X_ADDR_NM AS LocNm from SUPPLIER_HUB.C_BO_PSTL_ADDR WHERE ROWID_OBJECT ="
					+ rowIdobject + "";
			
			log.info("Before C_BO_PSTL_ADDR Select "+sql2);
			DBArs = stmt
					.executeQuery("select X_ADDR_NM AS LocNm from SUPPLIER_HUB.C_BO_PSTL_ADDR WHERE ROWID_OBJECT ="
							+ rowIdobject + "");
			String LocID = "";
			String LocNm = "";
			while (DBArs.next()) {
				LocID = rowIdobject;
				log.info("LocID " + LocID);
				LocNm = DBArs.getString("LocNm");
				log.info("LocNm " + LocNm);
			}
			
			/*LocNm = LocNm.replace("&", "&amp;");
			log.info("LocNm after replace - " + LocNm);*/

			if (LocID != null) {
				log.info("Update Nothing here");
				Map<String, String> soapmap = new HashMap();
					log.info("After soap call "+ new Date());
					String rID="";
					String rXrefID="";
					String sourceKeyID="";
					rID= soapmap.get("rowid");
					rXrefID=soapmap.get("rowidXref");
					sourceKeyID=soapmap.get("sourceKey");
					log.info("After soap call "+ rID);
					log.info("After soap call "+ rXrefID);
					log.info("After soap call "+ sourceKeyID);
					
					/*stmt2.executeUpdate("UPDATE SUPPLIER_HUB.C_XR_BNK_ADDR_REL"
							+" SET X_ADDR_NM='"+LocNm+"'"
							+" WHERE X_LOC_ID="+LocID+"");
					log.info("After REL update");
					
					stmt3.executeUpdate("UPDATE SUPPLIER_HUB.C_XR_BNK_ADDR_REL_XREF"
							+" SET X_ADDR_NM='"+LocNm+"'"
							+" WHERE X_LOC_ID="+LocID+"");
					log.info("After REL XREF update");*/
				DBArs.close();
			}
			log.info("end PutCallForLookupBnkDtlsUpdate ");
		} catch (Exception e) {
			log.info("Exception in PutCallForLookupBnkDtlsUpdate" + e);
		}

	}

	public Map<String, String> soapLocInsertBnkDtls(String XLOCATION_ID, String XLOCATION_NAME, String XPRTY_FK,String tablename) {
		
		Map<String, String> map = new HashMap();
		try {
			log.info("Inside soapLocInsert Banks Details ");
			// String query_url =
			// EnvironmentVariable.env_url;
			String xml = "<?xml version=\"1.0\" encoding =\"utf-8\"?>"
					+ " <soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:urn=\"urn:siperian.api\">"
					+ " <soapenv:Header/>" + " <soapenv:Body>" + " <urn:put>" + " <!--Optional:-->"
					+ " <urn:username>admin</urn:username>" + " <!--Optional:-->" + " <urn:password>"
					+ " <urn:password>"+ EnvironmentVariable.env_pswd +"/urn:password>" + " <urn:encrypted>false</urn:encrypted>"
					+ " </urn:password>" + " <!--Optional:-->" + " <urn:orsId>orcl-SUPPLIER_HUB</urn:orsId>"
					+ " <urn:asynchronousOptions>"
					+ " <urn:isAsynchronous>false</urn:isAsynchronous>" + " </urn:asynchronousOptions>"
					+ " <urn:recordKey>" + " <!--Optional:-->" + " <urn:systemName>Admin</urn:systemName>"
					+ " </urn:recordKey>" + " <urn:record>"
					+ " <urn:field>"
					+ " <urn:stringValue>" + XLOCATION_ID
					+ "</urn:stringValue>" + " <urn:name>X_LOC_ID</urn:name>" + " </urn:field>"
					+ " <urn:field>"
					+ " <urn:stringValue>" + XLOCATION_NAME 
					+ "</urn:stringValue>" + " <urn:name>X_ADDR_NM</urn:name>" + " </urn:field>"
					+ " <urn:siperianObjectUid>BASE_OBJECT.C_XR_BNK_ADDR_REL</urn:siperianObjectUid>"
					+ " </urn:record>" + " <urn:generateSourceKey>true</urn:generateSourceKey>" + " </urn:put>"
					+ " </soapenv:Body>" + " </soapenv:Envelope>";
			// Create a StringEntity for the SOAP XML.
			log.info("Inside soapLocInsert "+xml);
			StringEntity stringEntity = new StringEntity(xml, "UTF-8");
			log.info("Inside soapLocInsert1 ");
			stringEntity.setChunked(true);
			log.info("Inside soapLocInsert2 ");
			// Request parameters and other properties.
			HttpPost httpPost = new HttpPost(
					EnvironmentVariable.env_url);
			log.info("Inside soapLocInsert3 ");
			httpPost.setEntity(stringEntity);
			log.info("Inside soapLocInsert4 ");
			httpPost.addHeader("Accept", "text/xml");
			log.info("Inside soapLocInsert5 ");
			httpPost.addHeader("SOAPAction", "add");
			log.info("Inside soapLocInsert6 ");
			// Execute and get the response.
			HttpClient httpClient = HttpClientBuilder.create().build();
			log.info("Inside soapLocInsert7 ");
			// HttpClient httpClient = new DefaultHttpClient();
			HttpResponse response = httpClient.execute(httpPost);
			log.info("Inside soapLocInsert8 ");
			String strResponse = null;
			if(!tablename.equals("C_XR_BNK_ADDR_REL"))
			{
				
				log.info("Inside soapLocInsert9 ");
				HttpEntity entity = response.getEntity();
				log.info("Inside soapLocInsert10 ");
				
				if (entity != null) {
					strResponse = EntityUtils.toString(entity);
				}
				log.info("SOAP Response " + strResponse);
			}
			log.info("Inside soapLocInsert10 ");
		} catch (Exception e) {
			log.info("Exception in soapLocInsert " + e);
		}
		return map;
	}
	
}
