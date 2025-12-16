package com.informatica.mdm.hubuserexit.postload;


import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.json.XML;

public class DbaNmId 
{
	public void DbaNameMethod(String rowIdobject, Statement stmt, String tablename) 
	{
		Logger log = Logger.getLogger(GeneratePrchsFrmId.class);
		
		{
			ResultSet dbars = null;
			ResultSet dbanmrs = null;
			ResultSet DataTable = null;
			
			try {
				
				dbars = stmt.executeQuery("SELECT X_ALT_NM_ID ALT_NM_ID FROM SUPPLIER_HUB.C_BO_PRTY_NM WHERE HUB_STATE_IND = 1 AND ROWID_OBJECT ="
								+ rowIdobject + "");
								
				String DbaNmId = "";
				while (dbars.next()) 
				{
					DbaNmId = dbars.getString("ALT_NM_ID");
					log.info("ALT_NM_ID " + DbaNmId);
				}
				
				dbanmrs = stmt.executeQuery("SELECT NM FROM SUPPLIER_HUB.C_BO_PRTY_NM PPD WHERE HUB_STATE_IND = 1 AND ROWID_OBJECT ="
								+ rowIdobject + "");
								
				String DbaNm = "";
				while (dbanmrs.next()) 
				{
					DbaNm = dbanmrs.getString("NM");
					log.info("NM " + DbaNm);
				}
				
				log.info("ALT_NM_ID " + DbaNmId);
				log.info("NM " + DbaNm);
				
				ResultSet newdbanmrs = null;
				
				newdbanmrs = stmt.executeQuery("SELECT X_PRTY_DBA_NM DBA_NM FROM SUPPLIER_HUB.C_XT_DDP_DBA_NM WHERE X_PRTY_DBA_NM_ID = '"
						+ DbaNmId +"'");
						
				String newDbaNmId = null;
				while (newdbanmrs.next()) 
				{
					newDbaNmId = newdbanmrs.getString("DBA_NM");
					log.info("DBA_NM of C_XT_DDP_DBA_NM  --> " + newDbaNmId);
					log.info("DBA_NM of C_BO_PRTY_NM  -->" + DbaNm);
				}
				if(newDbaNmId != null) 
				{
					log.info("NM of C_BO_PRTY_NM  --> " + DbaNm);
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_DDP_DBA_NM SET X_PRTY_DBA_NM ='"+ DbaNm +"'WHERE X_PRTY_DBA_NM_ID = '" + DbaNmId + "'");            
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_DDP_DBA_NM_XREF SET X_PRTY_DBA_NM ='"+ DbaNm +"'WHERE X_PRTY_DBA_NM_ID = '" + DbaNmId + "'");
/*					DataTable = stmt.executeQuery("SELECT ROWID_XREF, PKEY_SRC_OBJECT, ROWID_OBJECT from SUPPLIER_HUB.C_XT_DDP_DBA_NM_XREF WHERE X_PRTY_DBA_NM_ID ='"
							+ DbaNmId + "' AND HUB_STATE_IND IN (0,1)");
					
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
						Map<String, String> map = new HashMap();
						try {
							log.info("Inside soapDBAUpdate ");
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
									+ " <rowid>" + rowid + " </rowid>"
									+ " <sourceKey>" + pkeySrcObject + "</sourceKey>"
									+ " <rowidXref>" + rowidXref + "</rowidXref>"
									+ " </urn:recordKey>" + " <urn:record>"
									+ " <urn:field>"
									+ " <urn:stringValue>" + DbaNmId
									+ "</urn:stringValue>" + " <urn:name>X_PRTY_DBA_NM_ID</urn:name>" + " </urn:field>"
									+ " <urn:field>"
									+ " <urn:stringValue>" + DbaNm
									+ "</urn:stringValue>" + " <urn:name>X_PRTY_DBA_NM</urn:name>" + " </urn:field>"
									+ " <urn:siperianObjectUid>BASE_OBJECT.C_XT_DDP_DBA_NM</urn:siperianObjectUid>"
									+ " </urn:record>" + " <urn:generateSourceKey>false</urn:generateSourceKey>" + " </urn:put>"
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
						}
				catch (Exception e) 
						{
						log.info("Exception in DbaNmId" + e);
						}*/
				}
				
				log.info("end DbaNmId ");
							
				dbars.close();
				dbanmrs.close();
			    }
			
				
			catch (Exception e) 
				{
				log.info("Exception in DbaNmId" + e);
				}
		}
		
	}
}
