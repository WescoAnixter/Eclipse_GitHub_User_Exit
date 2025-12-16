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
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

public class AssociateContact {
	private static Logger log = Logger.getLogger(GenerateDBAaltID.class);

	public void AssociateContactLookup(String rowIdobject, String prtyRowIdobject, Statement stmt, String emailId,  String lastNm, String tablename) {
		ResultSet ChildTwo;
			try {	
				
					log.info("RowidObject ----> " + rowIdobject);
					
					ChildTwo = stmt.executeQuery("select X_PRTY_FK AS CHPRTYFK from SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT WHERE X_CNCT_ROW_ID ='"
									+ rowIdobject + "'");
					//print the query, log is not entering in while loop
					String childPrtyFk = null;
					log.info("Entering in while loop.");
					log.info("CHPRTYFK -  " + childPrtyFk);
					while (ChildTwo.next()) {
						log.info("We are in While loop.");
						childPrtyFk = ChildTwo.getString("CHPRTYFK");
						log.info("CHPRTYFK -  " + childPrtyFk);
					}
					
					if(childPrtyFk != null){
						log.info("Update Nothing Here.");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT SET X_EMAIL_ADDR = '" + emailId + "' WHERE trim(X_CNCT_ROW_ID) IN (SELECT trim(ROWID_OBJECT) FROM SUPPLIER_HUB.C_XO_PRTY_CNTCT "
								+ "WHERE ROWID_OBJECT = '" +rowIdobject+ "')");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT_XREF"
								+" SET X_EMAIL_ADDR = '" + emailId + "' WHERE trim(X_CNCT_ROW_ID) IN (SELECT trim(ROWID_OBJECT) FROM SUPPLIER_HUB.C_XO_PRTY_CNTCT "
								+ "WHERE ROWID_OBJECT = '" +rowIdobject+ "')");
					}
					else
					{
						log.info("Insert XT table :");
						Map<String, String> soapmap = new HashMap();
						soapmap = soapAssociateContactXTInsert(rowIdobject, emailId, prtyRowIdobject, tablename);
						log.info("After soap call ");
						
						log.info("UPDATE SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT"
								+" SET X_PRTY_FK='"+prtyRowIdobject+"'"
										+" WHERE X_CNCT_ROW_ID="+rowIdobject+"");
						
						//St hub state indicator to the same as parent 
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT"
						+" SET X_PRTY_FK='"+prtyRowIdobject+"'"
								+" WHERE X_CNCT_ROW_ID="+rowIdobject+"");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT_XREF"
						+" SET X_PRTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF cbpx WHERE ROWID_OBJECT ='"+prtyRowIdobject+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY), S_X_PRTY_FK='"+prtyRowIdobject+"'"
								+" WHERE X_CNCT_ROW_ID="+rowIdobject+"");
						
						log.info("After REL XREF update");
						log.info("Inserted in X_PMNT_LOC_NM ");
					}
				}
				
			 catch (Exception e) {
			log.info("Exception in AssociateContactPmntDtls" + e);
			}
}
		
	public Map<String, String> soapAssociateContactXTInsert(String rowIdobject, String emailId, String prtyRowIdobject,String tablename) {
			Map<String, String> map = new HashMap();
			try {
				log.info("Inside soapAssociateContactXTInsert ");
				// String query_url =
				// "wesco@c51bhttps://wesco-devint.mdm.informaticahostednp.com:443/cmx/services/SifService";
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
						+ " <urn:stringValue>" + rowIdobject
						+ "</urn:stringValue>" + " <urn:name>X_CNCT_ROW_ID</urn:name>" + " </urn:field>"
						+ " <urn:field>"
						+ " <urn:stringValue>" + emailId
						+ "</urn:stringValue>" + " <urn:name>X_EMAIL_ADDR</urn:name>" + " </urn:field>"
						+ " <urn:siperianObjectUid>BASE_OBJECT.C_XT_LKP_PMNT_CNCT</urn:siperianObjectUid>"
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
				if(!tablename.equals("C_XT_LKP_PMNT_CNCT"))
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
				log.info("Exception in soapAssociateContactXTInsert " + e);
			}
			return map;
		}
		
	public void AssociateContactPmntChild(String rowIdobject, String prtyRowIdobject1, Statement stmt, String emailId1,String emailAddr) {
		log.info("RowidObject -----> "+rowIdobject);
		log.info("Party_FK -----> "+prtyRowIdobject1);
		log.info("EmailId -----> "+emailId1);
		log.info("emailAddr1 -----> "+emailAddr);
		
			try {	
				if(rowIdobject != null && prtyRowIdobject1 != null) {
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_CNCT"
					+" SET X_PMNT_CNCT_PRTY_FK='"+prtyRowIdobject1+"', X_CNCT_LAST_NM=(SELECT X_LST_NM FROM SUPPLIER_HUB.C_XO_PRTY_CNTCT WHERE ROWID_OBJECT  = '" + emailAddr
					+"') WHERE ROWID_OBJECT='"+rowIdobject+"'");
					
					log.info("After C_XO_PRTY_PMNT_CNCT update");
					
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_CNCT_XREF"
					+" SET X_CNCT_LAST_NM = (SELECT X_LST_NM FROM SUPPLIER_HUB.C_XO_PRTY_CNTCT WHERE ROWID_OBJECT  = '" + emailAddr
					+"'), X_PMNT_CNCT_PRTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF cbpx WHERE ROWID_OBJECT ='"+prtyRowIdobject1+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY), S_X_PMNT_CNCT_PRTY_FK='"+prtyRowIdobject1+"'"
							+" WHERE ROWID_OBJECT="+rowIdobject+"");
					log.info("After C_XO_PRTY_PMNT_CNCT xref  update");
								
				}
			}
			
			 catch (Exception e) {
			log.info("Exception in AssociateContactPmntDtls" + e);
			}
	}
	
	
	public void AssociateContactPmntAccuTechChild(String rowIdobject, String prtyRowIdobject1, Statement stmt, String emailId1,String emailAddr1) {
		log.info("RowidObject -----> "+rowIdobject);
		log.info("Party_FK -----> "+prtyRowIdobject1);
		log.info("EmailId -----> "+emailId1);
		log.info("emailAddr1 -----> "+emailAddr1);
		
			try {	
				if(rowIdobject != null && prtyRowIdobject1 != null) {
					
					log.info("SQL -----> "+emailId1);

					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_CNCT_ACCU_TECH"
							+" SET X_PMNT_CNT_ACCUTCH_PRTY_FK='"+prtyRowIdobject1+"', X_CNTCT_LAST_NM=(SELECT X_LST_NM FROM SUPPLIER_HUB.C_XO_PRTY_CNTCT WHERE ROWID_OBJECT  = '" + emailAddr1
							+"' AND X_PRTY_FK ='" + prtyRowIdobject1+"') WHERE ROWID_OBJECT='"+rowIdobject+"'");
							
							log.info("After C_XO_PMNT_CNCT_ACCU_TECH update");

							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_CNCT_ACCU_TECH_XREF"
							+" SET  X_CNTCT_LAST_NM=(SELECT X_LST_NM FROM SUPPLIER_HUB.C_XO_PRTY_CNTCT WHERE ROWID_OBJECT  = '" + emailAddr1
							+"'), X_PMNT_CNT_ACCUTCH_PRTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF cbpx WHERE ROWID_OBJECT ='"+prtyRowIdobject1+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY), S_X_PMNT_CNT_ACCUTCH_PRTY_FK='"+prtyRowIdobject1+"'"
									+" WHERE ROWID_OBJECT="+rowIdobject+"");
							log.info("After C_XO_PMNT_CNCT_ACCU_TECH_XREF update");
				}
			}
			
			 catch (Exception e) {
			log.info("Exception in AssociateContactPmntDtlsACcutech" + e);
			}
	}
}
       



