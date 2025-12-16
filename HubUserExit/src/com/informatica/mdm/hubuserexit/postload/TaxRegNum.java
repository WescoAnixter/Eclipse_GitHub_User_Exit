package com.informatica.mdm.hubuserexit.postload;

import java.sql.ResultSet;
import java.sql.Statement;
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

public class TaxRegNum {
	private static Logger log = Logger.getLogger(GenerateDBAaltID.class);

	public void TaxRegNumLookup(String rowIdobject, Statement stmt, String prtyrowIdobject, String taxRegNum, String tablename) {
		ResultSet taxXt;
		try {
			log.info("RowidObject ----> " + rowIdobject);
			log.info("taxRegNum ----> " + taxRegNum);
			taxXt = stmt.executeQuery("SELECT X_PMNT_TAX_RGSTR_NUM from SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM WHERE X_PMNT_TAX_RGSTR_ROWID ='"
							+ rowIdobject + "'");
			
			String taxRegXt = null;
			log.info("X_PMNT_TAX_RGSTR_NUM -  " + taxRegXt);
			
			while (taxXt.next()) {
				log.info("We are in While loop.");
				taxRegXt = taxXt.getString("X_PMNT_TAX_RGSTR_NUM");
				log.info("X_PMNT_TAX_RGSTR_NUM -  " + taxRegXt);
			}
			
			if(taxRegXt != null) {
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM"
						+" SET X_PMNT_TAX_RGSTR_NUM='"+taxRegXt+"'"
								+" WHERE X_PMNT_TAX_RGSTR_ROWID="+rowIdobject+"");
						
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM_XREF"
						+" SET X_PMNT_TAX_RGSTR_NUM='"+taxRegXt+"'"
								+" WHERE X_PMNT_TAX_RGSTR_ROWID="+rowIdobject+"");
				
			}
			else {
				log.info("Insert into XT table :");
				Map<String, String> soapmap = new HashMap();
				soapmap = soapTaxRegNum(rowIdobject, taxRegNum, tablename);
				log.info("After soap call ");
				
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM"
						+" SET X_PARTY_FK='"+prtyrowIdobject+"'"
								+" WHERE X_PMNT_TAX_RGSTR_ROWID="+rowIdobject+"");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM_XREF"
						+" SET X_PARTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF cbpx WHERE ROWID_OBJECT ='"+prtyrowIdobject+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY), S_X_PRTY_FK='"+prtyrowIdobject+"'"
								+" WHERE X_PMNT_TAX_RGSTR_ROWID="+rowIdobject+"");
			}
		}
		 catch (Exception e) {
			log.info("Exception in AssociateContactPmntDtls" + e);
		}
	}
	
	public void TaxRegNumUpdate(String rowIdobject, Statement stmt, String prtyrowIdobject, String taxRegNum, String tablename) {
		ResultSet taxId;
		try {
			log.info("RowidObject ----> " + rowIdobject);
			log.info("taxRegNum ----> " + taxRegNum);
			taxId = stmt.executeQuery("SELECT TAX_NUM from SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS WHERE ROWID_OBJECT ='" + rowIdobject + "'");
			
			String taxNumXt = null;
			log.info("TAX_NUM -  " + taxNumXt);
			
			while (taxId.next()) {
				log.info("We are in While loop.");
				taxNumXt = taxId.getString("TAX_NUM");
				log.info("TAX_NUM -  " + taxNumXt);
			}
			
			if(taxNumXt != null) {
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM"
						+" SET X_PMNT_TAX_RGSTR_NUM='"+taxNumXt+"'"
								+" WHERE X_PMNT_TAX_RGSTR_ROWID="+rowIdobject+"");
						
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM_XREF"
						+" SET X_PMNT_TAX_RGSTR_NUM='"+taxNumXt+"'"
								+" WHERE X_PMNT_TAX_RGSTR_ROWID="+rowIdobject+"");
				
			}
			else {
				log.info("Do nothing update");
			}
		}
		 catch (Exception e) {
			log.info("Exception in AssociateContactPmntDtls" + e);
		}
	}
	
	
	public Map<String, String> soapTaxRegNum(String rowIdobject, String taxRegNum, String tablename) {
		Map<String, String> map = new HashMap();
		try { 
			log.info("Inside soapTaxRegNum ");
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
					+ "</urn:stringValue>" + " <urn:name>X_PMNT_TAX_RGSTR_ROWID</urn:name>" + " </urn:field>"
					+ " <urn:field>"
					+ " <urn:stringValue>" + taxRegNum
					+ "</urn:stringValue>" + " <urn:name>X_PMNT_TAX_RGSTR_NUM</urn:name>" + " </urn:field>"
					+ " <urn:siperianObjectUid>BASE_OBJECT.C_XT_PMNT_TAX_RGSTR_NUM</urn:siperianObjectUid>"
					+ " </urn:record>" + " <urn:generateSourceKey>true</urn:generateSourceKey>" + " </urn:put>"
					+ " </soapenv:Body>" + " </soapenv:Envelope>";
			// Create a StringEntity for the SOAP XML.
			log.info("Inside soapTaxRegNum "+xml);
			StringEntity stringEntity = new StringEntity(xml, "UTF-8");
			log.info("Inside soapTaxRegNum1 ");
			stringEntity.setChunked(true);
			log.info("Inside soapTaxRegNum2 ");
			// Request parameters and other properties.
			HttpPost httpPost = new HttpPost(EnvironmentVariable.env_url);
			log.info("Inside soapTaxRegNum3 ");
			httpPost.setEntity(stringEntity);
			log.info("Inside soapTaxRegNum4 ");
			httpPost.addHeader("Accept", "text/xml");
			log.info("Inside soapTaxRegNum5 ");
			httpPost.addHeader("SOAPAction", "add");
			log.info("Inside soapTaxRegNum6 ");
			// Execute and get the response.
			HttpClient httpClient = HttpClientBuilder.create().build();
			log.info("Inside soapTaxRegNum7 ");
			// HttpClient httpClient = new DefaultHttpClient();
			HttpResponse response = httpClient.execute(httpPost);
			log.info("Inside soapTaxRegNum8 ");
			String strResponse = null;
			if(!tablename.equals("C_XT_PMNT_TAX_RGSTR_NUM"))
			{
				
				log.info("Inside soapTaxRegNum9 ");
				HttpEntity entity = response.getEntity();
				log.info("Inside soapTaxRegNum10 ");
				
				if (entity != null) {
					strResponse = EntityUtils.toString(entity);
				}
				log.info("SOAP Response " + strResponse);
			}
			log.info("Inside soapTaxRegNum11 ");
		} catch (Exception e) {
			log.info("Exception in soapTaxRegNum " + e);
		}
		return map;
	}
}
