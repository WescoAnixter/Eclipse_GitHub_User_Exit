package com.informatica.mdm.hubuserexit.postload;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
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

public class GenerateSuppNumId 
{
	private static Logger log = Logger.getLogger(GeneratePrchsFrmId.class);
	
	@SuppressWarnings("resource")
	public void GenerateUniqueSuppNumId(String rowIdobject, Statement stmt, Statement stmt1, String XPRTY_FK, String tablename)
	{
		ResultSet WLErs = null;
		ResultSet wrs = null;
		ResultSet conrs = null;
		ResultSet xpt = null;
		ResultSet srs = null;
		try {
			
			log.info("GenerateUniqueSuppNumId Entry Point");
			log.info("SQL1-- SELECT X_PRTY_TYP AS X_PRTY_TYP,CREATOR,FULL_NM, HUB_STATE_IND FROM SUPPLIER_HUB.C_BO_PRTY cbp WHERE ROWID_OBJECT ="
					+ rowIdobject + "");
			xpt= stmt.executeQuery("SELECT X_PRTY_TYP AS X_PRTY_TYP,CREATOR,FULL_NM,HUB_STATE_IND,X_SUPP_NUM FROM SUPPLIER_HUB.C_BO_PRTY cbp WHERE ROWID_OBJECT ="
							+ rowIdobject + "");
			
			String prtyTyp = "";
			String Creator="";
			String FullNm="";
			String HSI="";
			String Snum = "";
			while (xpt.next()) {
				prtyTyp = xpt.getString("X_PRTY_TYP");
				//Creator = xpt.getString("CREATOR");
				FullNm = xpt.getString("FULL_NM");
				HSI = xpt.getString("HUB_STATE_IND");
				Snum = xpt.getString("X_SUPP_NUM");
				
				log.info("X_PRTY_TYP (line 41), Creator,HSI " + prtyTyp+ Creator+HSI);
			}
			
		//Logic for HSI=1
			if(HSI.equals("1") && (Snum == null || Snum.isEmpty())) {
		WLErs = stmt.executeQuery("SELECT X_SUPP_NUM_TEMP AS SUPP_NUM_TEMP FROM SUPPLIER_HUB.C_BO_PRTY cbp WHERE HUB_STATE_IND = 1 AND ROWID_OBJECT ="
						+ rowIdobject + "");
		String SuppNmId = "";
		while (WLErs.next()) {
			SuppNmId = WLErs.getString("SUPP_NUM_TEMP");
			log.info("SUPP_NUM_TEMP (line 22) " + SuppNmId);
		     }
		/*if(prtyTyp.equals("Wesco Legal Entity") && (SuppNmId==null||SuppNmId.equals("")||SuppNmId.isEmpty())){
	
					conrs = stmt.executeQuery("SELECT MAX(X_SUPP_NUM_TEMP) AS MAX_SUPP_NUM_TEMP FROM SUPPLIER_HUB.C_BO_PRTY cbp WHERE X_SUPP_NUM_TEMP LIKE 'WLE%'");
					String altNmIdWle="";
					String maxWLE = "";

					while (conrs.next()) 
					{
						altNmIdWle = conrs.getString("MAX_SUPP_NUM_TEMP");
						log.info("MAX_SUPP_NUM_TEMP (WLE)" + altNmIdWle);
					}
						int numberWle = Integer.parseInt(altNmIdWle.substring(altNmIdWle.length()-4));
						numberWle = numberWle + 1;
						String numbersWle = String.format("%04d", numberWle);
						String initialWle = "WLE";
						initialWle = initialWle.concat(numbersWle);
						maxWLE = initialWle;
						
						log.info("X_SUPP_NUM_TEMP after increment " + maxWLE);
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_SUPP_NUM = '" + maxWLE
								+ "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET X_SUPP_NUM = '" + maxWLE
								+ "' WHERE ROWID_OBJECT =" + rowIdobject + "");
		
			}*/
			if (SuppNmId==null||SuppNmId.equals("")||SuppNmId.isEmpty()){
				srs = stmt.executeQuery("SELECT MAX(X_SUPP_NUM_TEMP) AS MAX_SUPP_NUM_TEMP FROM SUPPLIER_HUB.C_BO_PRTY");
				log.info("in else if part SM ");
				String updatedSuppNm2 = "";
				String altNmId2="";
				
				while (srs.next()) 
				{
					altNmId2 = srs.getString("MAX_SUPP_NUM_TEMP");
					log.info("MAX_SUPP_NUM_TEMP2 - " + altNmId2);
				}
				
					log.info("MAX_SUPP_NUM_TEMP2 (SM) " + altNmId2);

					int number2 = Integer.parseInt(altNmId2.substring(altNmId2.length()-10));
					number2 = number2 + 1;
					String numbers2 = Integer.toString(number2);
					String initial2 = "SM";
					initial2 = initial2.concat(numbers2);
					updatedSuppNm2 = initial2;
					
					log.info("X_SUPP_NUM_TEMP after increment " + updatedSuppNm2);	
					
					log.info("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_SUPP_NUM = '" + updatedSuppNm2+ "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_SUPP_NUM = '" + updatedSuppNm2
							+ "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					log.info("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET X_SUPP_NUM = '" + updatedSuppNm2+ "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET X_SUPP_NUM = '" + updatedSuppNm2
							+ "' WHERE ROWID_OBJECT =" + rowIdobject + "");	
					
					log.info("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_SUPP_NUM_TEMP = '" + updatedSuppNm2+ "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_SUPP_NUM_TEMP = '" + updatedSuppNm2
							+ "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					log.info("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET X_SUPP_NUM_TEMP = '" + updatedSuppNm2+ "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET X_SUPP_NUM_TEMP = '" + updatedSuppNm2
							+ "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					
					
					//Update C_L_S360_CROSS_REF table
					
					/*if(Creator != "avos_user" && Creator != "svc_s360_batchexec"){
						if(FullNm.contains("'")){
							FullNm = FullNm.replace("'", "''");
							log.info("Full Name - " + FullNm);
						}
						log.info("Insert in Cross Ref 184");
						stmt.executeUpdate("INSERT INTO SUPPLIER_HUB.C_L_S360_SDH_CROSS_REF (X_SUPP_NUM, X_SUPP_NM) VALUES ( '" + updatedSuppNm2 + "','" + FullNm + "')");
					      log.info("After C_L_S360_SDH_CROSS_REF insert 119");
					}
					else{
						log.info("Do Nothing Here. Line 122");
					}*/
				
			}
		}
		}catch (Exception e) 
		{
			log.info("Exception in GenerateSuppNumId" + e);
		}
		
	 }
	
}
