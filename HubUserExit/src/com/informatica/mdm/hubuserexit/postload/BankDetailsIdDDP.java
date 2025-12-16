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

public class BankDetailsIdDDP {
	private static Logger log = Logger.getLogger(GenerateDBAaltID.class);
	
	public void BankDetailsDDPLookup(String rowIdobject, Statement stmt, String prtyrowIdobject, String tablename) {
		ResultSet BankDDPxt;
		try {
			log.info("DDP RowidObject and BankDetailsID ----> " + rowIdobject);
			log.info("Prty Fk ----> " + prtyrowIdobject);
			
			BankDDPxt = stmt.executeQuery("SELECT X_PMNT_BNK_DTLS_DDP_ROWID from SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID WHERE X_PMNT_BNK_ACC_ID ='"
							+ rowIdobject + "'");
			
			String pmntBnkDtlsRowid = null;
			log.info("Party Fk -  " + pmntBnkDtlsRowid);
			
			while (BankDDPxt.next()) {
				log.info("We are in While loop.");
				pmntBnkDtlsRowid = BankDDPxt.getString("X_PMNT_BNK_DTLS_DDP_ROWID");
				log.info("Payment Bank Details DDp -  " + pmntBnkDtlsRowid);
			}
			
			if(pmntBnkDtlsRowid != null) {
				log.info("Update nothing here, No Bank ID");
			}
			else {
				log.info("Insert into XT table :");
				Map<String, String> soapmap = new HashMap();
				soapmap = soapBankDetailsID(rowIdobject, tablename);
				log.info("After soap call ");
				
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID "
						+ "SET X_PRTY_FK='"+prtyrowIdobject+"'"
								+" WHERE X_PMNT_BNK_DTLS_DDP_ROWID="+rowIdobject+"");
						
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID_XREF"
						+" SET X_PRTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF cbpx WHERE ROWID_OBJECT ='"+prtyrowIdobject+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY), S_X_PRTY_FK='"+prtyrowIdobject+"'"
								+" WHERE X_PMNT_BNK_DTLS_DDP_ROWID="+rowIdobject+"");
			}
		}
		 catch (Exception e) {
			log.info("Exception in AssociateContactPmntDtls" + e);
		}
	}
	
	public Map<String, String> soapBankDetailsID(String rowIdobject, String tablename) {
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
					+ "</urn:stringValue>" + " <urn:name>X_PMNT_BNK_ACC_ID</urn:name>" + " </urn:field>"
					+ " <urn:field>"
					+ " <urn:stringValue>" + rowIdobject
					+ "</urn:stringValue>" + " <urn:name>X_PMNT_BNK_DTLS_DDP_ROWID</urn:name>" + " </urn:field>"
					+ " <urn:siperianObjectUid>BASE_OBJECT.C_XT_PMNT_BANK_ACC_ID</urn:siperianObjectUid>"
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
			if(!tablename.equals("C_XT_PMNT_BANK_ACC_ID"))
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
