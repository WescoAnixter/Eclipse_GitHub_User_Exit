package com.informatica.mdm.hubuserexit.postload;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import java.util.Base64.Decoder;
import java.util.Base64;

import org.apache.log4j.Logger;

import com.informatica.mdm.api.put.ActionType;
import com.informatica.mdm.userexit.UserExitContext;
import java.text.Normalizer;
import java.text.SimpleDateFormat;

public class PostLoad implements com.informatica.mdm.userexit.PostLoadUserExit {
	private static Logger log = Logger.getLogger(PostLoad.class);

	@Override
	public void processUserExit(UserExitContext userExitContext, ActionType actionType,
			Map<String, Object> baseObjectDataMap, Map<String, Object> xrefDataMap,
			List<Map<String, Object>> xrefDataMapList) {
		
		log.info("POST_LOAD baseObjectDataMap " + baseObjectDataMap);
		log.info("POST_LOAD userExitContext " + userExitContext.getBatchJobRowid());
		log.info("POST_LOAD process started");
		String tablename = userExitContext.getTableName();
		String rowIdobject = null;
		String prtyRowIdobject = null;
		log.info("POST_LOAD table name Entry Point " + tablename);
		
		String stgTablename=userExitContext.getStagingTableName();
		log.info("POST_LOAD process stgTablename "+stgTablename);
		
		
if (stgTablename == null) {
//-------------------------------------------------------C_XO_ADDR_DNB_VLDN-----------------------------------------------------------------
		log.info("To delete DUNS match ");
		
		
		
		if (tablename != null && tablename.equals("C_XO_ADDR_DNB_VLDN")) 
		{
			log.info("DNB_POST_LOAD inside table " + tablename);

			Set<String> setBOKey = baseObjectDataMap.keySet();
			
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								log.info("DNB_POST_LOAD rowIdObject" + stValue);
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("DNB_POST_LOAD rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
								log.info("DNB_POST_LOAD rowIdObject" + stValue);
								prtyRowIdobject = stValue;
								prtyRowIdobject = prtyRowIdobject.trim();
								log.info("DNB_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
							}
						}
					}
				}
			}
			
			log.info("DNB_Begin DB Connection creation DNB");
			
				//Ankit Date:31/10/2025
				
			DatabaseHelper.performDBOperation(prtyRowIdobject, "DNB");
					
		}
//-----------------------------------------------------------C_XO_VSUL_CMPLNC_VLDN-----------------------------------------------------------------
		log.info("To delete VC match ");
		
		if (tablename != null && tablename.equals("C_XO_VSUL_CMPLNC_VLDN")) {
			log.info("VC_POST_LOAD inside table " + tablename);

			Set<String> setBOKey1 = baseObjectDataMap.keySet();
			if (setBOKey1 != null) {
				Iterator<String> itrBOKey = setBOKey1.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								log.info("VC_POST_LOAD rowIdObject" + stValue);
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("VC_POST_LOAD rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
								log.info("VC_POST_LOAD rowIdObject" + stValue);
								prtyRowIdobject = stValue;
								prtyRowIdobject = prtyRowIdobject.trim();
								log.info("VC_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
							}
						}
					}
				}
			}
			
			//Ankit Date:31/10/2025
			 
			log.info("VC_Begin DB Connection creation DNB");
			DatabaseHelper.performDBOperation(prtyRowIdobject, "DNB");
			
			
		}
//-------------------------------------------------------C_XO_EX_SUPP_REC-------------------------------------------------------------------------
		log.info("To delete Exisiting Supplier match ");
		if (tablename != null && tablename.equals("C_XO_EX_SUPP_REC")) {
			log.info("ExistSupp_POST_LOAD inside table " + tablename);

			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								log.info("ExistSupp_POST_LOAD rowIdObject" + stValue);
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("ExistSupp_POST_LOAD rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
								log.info("ExistSupp_POST_LOAD rowIdObject" + stValue);
								prtyRowIdobject = stValue;
								prtyRowIdobject = prtyRowIdobject.trim();
								log.info("ExistSupp_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
							}
						}
					}
				}
			}
			log.info("ExistSupp_Begin DB Connection creation DNB");
		
		//Ankit Date:06/11/2025
			
			log.info("Begin DB ExistSupp_Begin Connection creation ");
			
			DatabaseConnection dbHandler = new DatabaseConnection();
            Statement stmt = null;

    try {
        stmt = dbHandler.createConnection();
        log.info("ExistSupp_Before PendingDeleteUpdate ");
		PendingDeleteUpdate pdu = new PendingDeleteUpdate();
		pdu.pendingUpdateExistingSupp(prtyRowIdobject, stmt);
    } catch (Exception e) {
   	 log.info("Exception in ExistSupp_Exception DB connection creation " + e);    

    } finally {
        dbHandler.closeResources();
    }  
    
          //End Ankit Date:06/11/2025
			
		}
		

//------------------------------------------------------------C_BO_PRTY---------------------------------------------------------
		log.info("Line 210 TableName: " + tablename);
		// Generating objectId for C_BO_PRTY
		String endDateNull = null;
		if (tablename != null && tablename.equals("C_BO_PRTY")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_BO_PRTY_POST_LOAD rowIdObject" + rowIdobject);
							}
						}
					}
				}
			}
			log.info("Begin DB C_BO_PRTY Connection creation 236");
			Context ctx = null;
			Connection con = null;
			Statement stmt = null;
			Connection con1 = null;
			Statement stmt1 = null;
			Statement stmt2 = null;
			Connection con2 = null;
			try {
				ctx = new InitialContext();
				DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
				con = ds.getConnection();
				stmt = con.createStatement();
				con1 = ds.getConnection();
				stmt1 = con1.createStatement();
				con2 = ds.getConnection();
				stmt2 = con2.createStatement();
				
				
				ResultSet MainT = stmt1.executeQuery("SELECT DISTINCT cxsmd.X_RQST_STS  FROM  SUPPLIER_HUB.C_BO_PRTY prty " +
						"JOIN SUPPLIER_HUB.C_XO_SUPP_MAINTNC_DTLS cxsmd ON prty.ROWID_OBJECT = cxsmd.X_PRTY_FK JOIN SUPPLIER_HUB.C_BO_PRTY_XREF xrf " +
						"ON xrf.rowid_object=prty.ROWID_OBJECT WHERE xrf.HUB_STATE_IND=0 AND xrf.ROWID_SYSTEM='SYS0' " +
						"AND xrf.full_nm<>prty.FULL_NM AND cxsmd.X_RQST_STS='Pending' AND prty.ROWID_OBJECT = " + rowIdobject + "");
				
				log.info("SELECT DISTINCT cxsmd.X_RQST_STS  FROM  SUPPLIER_HUB.C_BO_PRTY prty " +
						"JOIN SUPPLIER_HUB.C_XO_SUPP_MAINTNC_DTLS cxsmd ON prty.ROWID_OBJECT = cxsmd.X_PRTY_FK JOIN SUPPLIER_HUB.C_BO_PRTY_XREF xrf " +
						"ON xrf.rowid_object=prty.ROWID_OBJECT WHERE xrf.HUB_STATE_IND=0 AND xrf.ROWID_SYSTEM='SYS0' " +
						"AND xrf.full_nm<>prty.FULL_NM AND cxsmd.X_RQST_STS='Pending' AND prty.ROWID_OBJECT = " + rowIdobject + "");
				
				String rqstSts = null;
				
					while (MainT.next()) {
                    log.info("We are in While loop.");
                    rqstSts = MainT.getString("X_RQST_STS");
                    log.info("X_RQST_STS -  " + rqstSts);
					}
				
				 ResultSet MainT2 = stmt2.executeQuery("SELECT X_RQST_STS FROM SUPPLIER_HUB.C_XO_SUPP_MAINTNC_DTLS WHERE X_PRTY_FK ='" + rowIdobject + "' AND X_RQST_STS IN ('Pending','Pending Approval')");
				 
				 String requestS = null;
				 while (MainT2.next()) {
	                    log.info("We are in While loop.");
	                    requestS = MainT2.getString("X_RQST_STS");
	                    log.info("X_RQST_STS -  " + requestS);
						}
				
				
				if(rqstSts != null || requestS != null) {
					log.info("Supplier IN Maintenance");
					ResultSet normSuppNm = stmt1.executeQuery("SELECT FULL_NM, X_INACTV_DT, X_PRTY_STS FROM SUPPLIER_HUB.C_BO_PRTY_XREF WHERE ROWID_OBJECT = '" + rowIdobject + "' AND HUB_STATE_IND = '0'");
					String suppNmx = null;
					String prtyStatus = null;
					String inactDt = null;
					while (normSuppNm.next()) {
	                    log.info("We are in While loop.");
	                    suppNmx = normSuppNm.getString("FULL_NM");
	                    log.info("FULL_NM -  " + suppNmx);
	                    inactDt = normSuppNm.getString("X_INACTV_DT");
	                    log.info("X_INACTV_DT -  " + inactDt);
	                    prtyStatus = normSuppNm.getString("X_PRTY_STS");
	                    log.info("X_PRTY_STS -  " + prtyStatus);
	                }
					
					if(suppNmx != null) {
					String full_nm_x = ( Normalizer.normalize(suppNmx.toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
					log.info("Normalized Full Name : "+ suppNmx);
					//stmt1.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET FULL_NM = '" + full_nm_x + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					stmt1.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET FULL_NM = '" + full_nm_x + "' WHERE ROWID_OBJECT ='" + rowIdobject + "' AND HUB_STATE_IND = 0");
					}
				
					
	                if(!(prtyStatus.isEmpty()) && rowIdobject != null)
	                {
	                	if (prtyStatus.equals("Inactive") && inactDt == null)
	                    {
	                    	log.info("Party STS 1 - " + prtyStatus);
	                        //stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_INACTV_DT = SYSDATE WHERE ROWID_OBJECT = " + rowIdobject + "");
	                        stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET X_INACTV_DT = SYSDATE WHERE ROWID_OBJECT = " + rowIdobject + "");
	                        log.info("Parent End date set.");
	                        
	                    }
	                    
	                    log.info("Is not Inactive -" + !(prtyStatus.equals("Inactive")));
	                    log.info("Is not Approved -" + !(prtyStatus.equals("Approved")));
	                    
	                    if ((!(prtyStatus.equals("Pending Approval")) && !prtyStatus.equals("Approved")) ) {
	                    	if(inactDt == null) 
	                    {
	                    	log.info("Party STS 2 - " + prtyStatus);
	                    
	                    	//stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_INACTV_DT = SYSDATE WHERE ROWID_OBJECT = " + rowIdobject + "");
	                        stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET X_INACTV_DT = SYSDATE WHERE ROWID_OBJECT = " + rowIdobject + "");
	                        log.info("Parent End date set.");  
	                    }
	                    	}
	                    if(prtyStatus.equals("Approved") && inactDt != null)
	                    {
	                    	log.info("When Status and End Date is NOT NULL.");
	                    	log.info("End Date - " + inactDt);	
	                    	stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET X_PRTY_STS ='Inactive' WHERE X_INACTV_DT <= SYSDATE+1 AND X_INACTV_DT IS NOT NULL AND ROWID_OBJECT = " + rowIdobject + "");
	        	
	                    }
	                }
	                else
	                {
	                	log.info("Error in EndDate and Party Status loops.");
	                }
				}
				
				else if (rqstSts == null || rqstSts.isEmpty()) {
					log.info("Supplier NOT in Maintenance");
				String full_nm = ( Normalizer.normalize(baseObjectDataMap.get("FULL_NM").toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
				log.info("Normalized Full Name : "+full_nm);
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET FULL_NM = '" + full_nm + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET FULL_NM = '" + full_nm + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
				
				
				//Supplier Name update in all the dynamic lookups.
				log.info("Updating Supplier Name for Lookups.");
                ResultSet prty =  stmt.executeQuery("Select X_PRTY_FK from supplier_hub.C_XT_PRTY_FK where X_PRTY_FK = " + rowIdobject + "");
                
                String value01 = null;
                while (prty.next()) {
                    log.info("We are in While loop.");
                    value01 = prty.getString("X_PRTY_FK");
                    log.info("X_PRTY_FK -  " + value01);
                }
                log.info("Prty - " +value01);
                if (value01 != null)
                {
                    stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PRTY_FK SET X_SUPP_NM = '" + full_nm + "' WHERE X_PRTY_FK =" + rowIdobject + "");
                    log.info("Name Updated in XT_PRTY_FK.");
                }

                else {
                    log.info("Party not in C_XT_PRTY_FK Party_fk: " + rowIdobject );
                    }
				}
              //Supplier End Date get from BO_PRTY table
				log.info("Getting End Date from C_BO_PRTY");
                ResultSet enddate =  stmt.executeQuery("SELECT X_INACTV_DT FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT = '" + rowIdobject + "'");
                
                String EndDate = null;
                while (enddate.next()) {
                    log.info("We are in While loop.");
                    EndDate = enddate.getString("X_INACTV_DT");
                    log.info("X_INACTV_DT -  " + EndDate);
                }
                
                //End Date and Status update.
                log.info("Prty Sts -" + baseObjectDataMap.get("X_PRTY_STS").toString());
                log.info("Hub State Indicator -" + baseObjectDataMap.get("HUB_STATE_IND").toString());
                //log.info("Main Condition - " + !(baseObjectDataMap.get("HUB_STATE_IND").toString() == "0"));
                log.info("Value Status- " + (baseObjectDataMap.get("X_PRTY_STS").toString()).equals("Inactive")); 
                log.info("End date - " + baseObjectDataMap.get("X_INACTV_DT"));
                log.info("Rowid Object -" + rowIdobject);
                log.info("X_INACTV_DT -  " + EndDate);
                
                
                if(!(baseObjectDataMap.get("HUB_STATE_IND").toString() == "0")){ 
                    if(!(baseObjectDataMap.get("X_PRTY_STS").toString()).isEmpty() && rowIdobject != null)
                    {
                    	/*if ((baseObjectDataMap.get("X_PRTY_STS").toString()).equals("Inactive") && EndDate == null)
                        {
                        	log.info("Party STS 1 - " + baseObjectDataMap.get("X_PRTY_STS").toString());
                        
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_INACTV_DT = SYSDATE WHERE ROWID_OBJECT = " + rowIdobject + "");
                            log.info("Parent End date set.");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP SET X_SUPP_STS ='Inactive', X_SUPP_END_DT = SYSDATE WHERE X_PRTY_FK = " + rowIdobject + " AND X_SUPP_STS IS NOT NULL");
                        	stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP_XREF SET X_SUPP_STS ='Inactive', X_SUPP_END_DT = SYSDATE WHERE X_PRTY_FK = " + rowIdobject + " AND X_SUPP_STS IS NOT NULL");
                        	
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP SET X_SUPP_STS ='Inactive', X_SUPP_END_DT = SYSDATE WHERE X_PRTY_FK = " + rowIdobject + " AND X_SUPP_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP_XREF SET X_SUPP_STS ='Inactive', X_SUPP_END_DT = SYSDATE WHERE X_PRTY_FK = " + rowIdobject + " AND X_SUPP_STS IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_STS ='I', X_INACTIV_DT = SYSDATE WHERE ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ") AND X_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_STS ='I', X_INACTIV_DT = SYSDATE WHERE ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ") AND X_STS IS NOT NULL");
                            
                            //stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN SET X_STS ='PENDING', X_EXP_DT = SYSDATE WHERE X_PARTY_FK = " + rowIdobject + " AND X_STS IS NOT NULL");
                            //stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN_XREF SET X_STS ='PENDING', X_EXP_DT = SYSDATE WHERE X_PARTY_FK = " + rowIdobject + " AND X_STS IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT SET X_STS_CD ='I', X_INACT_DT = SYSDATE WHERE X_PRTY_FK = " + rowIdobject + " AND X_STS_CD IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT_XREF SET X_STS_CD ='I', X_INACT_DT = SYSDATE WHERE X_PRTY_FK = " + rowIdobject + " AND X_STS_CD IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS SET ACCNT_STS ='INACTIVE', EFF_END_DT = SYSDATE WHERE PRTY_FK = " + rowIdobject + " AND ACCNT_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS_XREF SET ACCNT_STS ='INACTIVE', EFF_END_DT = SYSDATE WHERE PRTY_FK = " + rowIdobject + " AND ACCNT_STS IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID SET STS_CD ='Expired', EFF_END_DT = SYSDATE WHERE PRTY_FK = " + rowIdobject + " AND STS_CD IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID_XREF SET STS_CD ='Expired', EFF_END_DT = SYSDATE WHERE PRTY_FK = " + rowIdobject + " AND STS_CD IS NOT NULL");  
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_RESALE_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_RESALE_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE_XREF SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_RESALE_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_RESALE_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_EXPNS_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_EXPNS_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE_XREF SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_EXPNS_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_EXPNS_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS SET ACCNT_STS ='INACTIVE', EFF_END_DT = SYSDATE WHERE PRTY_FK = " + rowIdobject + " AND ACCNT_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS_XREF SET ACCNT_STS ='INACTIVE', EFF_END_DT = SYSDATE WHERE PRTY_FK = " + rowIdobject + " AND ACCNT_STS IS NOT NULL"); 
                            
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                    		stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP_XREF SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                    		
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT SET X_INACT_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_INACT_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT_XREF SET X_INACT_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_INACT_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS_XREF SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN SET X_EXP_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PARTY_FK =" + rowIdobject + "AND X_EXP_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN_XREF SET X_EXP_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PARTY_FK =" + rowIdobject + "AND X_EXP_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS_XREF SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID_XREF SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM_XREF SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_INACTIV_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" )WHERE ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ") AND X_INACTIV_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_INACTIV_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" )WHERE ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ") AND X_INACTIV_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_OFFSYS_NM SET X_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_OFFSYS_NM_XREF SET X_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP_XREF SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");

                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                                                    
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PMNT_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PMNT_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS_XREF SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH SET X_END_DATE = (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH_XREF SET X_END_DATE = (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            //Supplier Sub-Role
                            
                            log.info("End of loop 1.");
                            
                        }
                        
                        log.info("Is not Inactive -" + !(baseObjectDataMap.get("X_PRTY_STS").toString()).equals("Inactive"));
                        log.info("Is not Approved -" + !(baseObjectDataMap.get("X_PRTY_STS").toString()).equals("Approved"));*/
                        
                        /*if ((!(baseObjectDataMap.get("X_PRTY_STS").toString()).equals("Pending Approval") && !(baseObjectDataMap.get("X_PRTY_STS").toString()).equals("Approved"))) {
                        	if(EndDate == null) {
                        		
                        	log.info("Party STS 2a - " + baseObjectDataMap.get("X_PRTY_STS").toString());
                        
                        	stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_INACTV_DT = SYSDATE WHERE ROWID_OBJECT = " + rowIdobject + "");
                        	log.info("End Date of C_BO_PRTY set.");
                        	
                        	stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP SET X_SUPP_STS ='Inactive', X_SUPP_END_DT = SYSDATE WHERE X_PRTY_FK = " + rowIdobject + " AND X_SUPP_STS IS NOT NULL");
                        	stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP_XREF SET X_SUPP_STS ='Inactive', X_SUPP_END_DT = SYSDATE WHERE X_PRTY_FK = " + rowIdobject + " AND X_SUPP_STS IS NOT NULL");
                        	
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP SET X_SUPP_STS ='Inactive', X_SUPP_END_DT = SYSDATE WHERE X_PRTY_FK = " + rowIdobject + " AND X_SUPP_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP_XREF SET X_SUPP_STS ='Inactive', X_SUPP_END_DT = SYSDATE WHERE X_PRTY_FK = " + rowIdobject + " AND X_SUPP_STS IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_STS ='I', X_INACTIV_DT = SYSDATE WHERE ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ") AND X_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_STS ='I', X_INACTIV_DT = SYSDATE WHERE ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ") AND X_STS IS NOT NULL");
                            
                            //stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN SET X_STS ='PENDING', X_EXP_DT = SYSDATE WHERE X_PARTY_FK = " + rowIdobject + " AND X_STS IS NOT NULL");
                            //stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN_XREF SET X_STS ='PENDING', X_EXP_DT = SYSDATE WHERE X_PARTY_FK = " + rowIdobject + " AND X_STS IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT SET X_STS_CD ='I', X_INACT_DT = SYSDATE WHERE X_PRTY_FK = " + rowIdobject + " AND X_STS_CD IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT_XREF SET X_STS_CD ='I', X_INACT_DT = SYSDATE WHERE X_PRTY_FK = " + rowIdobject + " AND X_STS_CD IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS SET ACCNT_STS ='INACTIVE', EFF_END_DT = SYSDATE WHERE PRTY_FK = " + rowIdobject + " AND ACCNT_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS_XREF SET ACCNT_STS ='INACTIVE', EFF_END_DT = SYSDATE WHERE PRTY_FK = " + rowIdobject + " AND ACCNT_STS IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID SET STS_CD ='Expired', EFF_END_DT = SYSDATE WHERE PRTY_FK = " + rowIdobject + " AND STS_CD IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID_XREF SET STS_CD ='Expired', EFF_END_DT = SYSDATE WHERE PRTY_FK = " + rowIdobject + " AND STS_CD IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS SET ACCNT_STS ='INACTIVE', EFF_END_DT = SYSDATE WHERE PRTY_FK = " + rowIdobject + " AND ACCNT_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS_XREF SET ACCNT_STS ='INACTIVE', EFF_END_DT = SYSDATE WHERE PRTY_FK = " + rowIdobject + " AND ACCNT_STS IS NOT NULL"); 
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_RESALE_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_RESALE_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE_XREF SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_RESALE_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_RESALE_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_EXPNS_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_EXPNS_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE_XREF SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_EXPNS_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_EXPNS_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            
                            
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                    		stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP_XREF SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                    		
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT SET X_INACT_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_INACT_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT_XREF SET X_INACT_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_INACT_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS_XREF SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN SET X_EXP_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PARTY_FK =" + rowIdobject + "AND X_EXP_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN_XREF SET X_EXP_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PARTY_FK =" + rowIdobject + "AND X_EXP_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS_XREF SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID_XREF SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM_XREF SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_INACTIV_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" )WHERE ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ") AND X_INACTIV_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_INACTIV_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" )WHERE ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ") AND X_INACTIV_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_OFFSYS_NM SET X_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_OFFSYS_NM_XREF SET X_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP_XREF SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");

                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PMNT_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PMNT_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS_XREF SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");

                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH SET X_END_DATE = (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH_XREF SET X_END_DATE = (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            log.info("End of loop 2a ");
                            
                        }
                        	
                        	if(EndDate != null) {
                        		
                        	log.info("Party STS 2b - " + baseObjectDataMap.get("X_PRTY_STS").toString());
                        
                        	log.info("When Status and End Date is NOT NULL. 2b");
                        	log.info("End Date - " + EndDate);
                        	
                    		stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                    		stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP_XREF SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                    		
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT SET X_INACT_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_INACT_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT_XREF SET X_INACT_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_INACT_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS_XREF SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN SET X_EXP_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PARTY_FK =" + rowIdobject + "AND X_EXP_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN_XREF SET X_EXP_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PARTY_FK =" + rowIdobject + "AND X_EXP_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS_XREF SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID_XREF SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM_XREF SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_INACTIV_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" )WHERE ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ") AND X_INACTIV_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_INACTIV_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" )WHERE ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ") AND X_INACTIV_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_OFFSYS_NM SET X_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_OFFSYS_NM_XREF SET X_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP_XREF SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PMNT_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PMNT_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS_XREF SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH SET X_END_DATE = (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH_XREF SET X_END_DATE = (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                         log.info("End Date updated in child records.");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_PRTY_STS ='Inactive' WHERE X_INACTV_DT <= SYSDATE+1 AND X_INACTV_DT IS NOT NULL AND ROWID_OBJECT = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP SET X_SUPP_STS ='Inactive' WHERE X_SUPP_END_DT <= SYSDATE+1 AND X_SUPP_END_DT IS NOT NULL AND X_PRTY_FK = " + rowIdobject + "");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP_XREF SET X_SUPP_STS ='Inactive' WHERE X_SUPP_END_DT <= SYSDATE+1 AND X_SUPP_END_DT IS NOT NULL AND X_PRTY_FK = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP SET X_SUPP_STS ='Inactive' WHERE X_SUPP_END_DT <= SYSDATE+1 AND X_SUPP_END_DT IS NOT NULL AND X_PRTY_FK = " + rowIdobject + "");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP_XREF SET X_SUPP_STS ='Inactive' WHERE X_SUPP_END_DT <= SYSDATE+1 AND X_SUPP_END_DT IS NOT NULL AND X_PRTY_FK = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_STS ='I' WHERE X_INACTIV_DT <= SYSDATE+1 AND X_INACTIV_DT IS NOT NULL AND ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ")");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_STS ='I' WHERE X_INACTIV_DT <= SYSDATE+1 AND X_INACTIV_DT IS NOT NULL AND ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ")");
                            
                            //stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN SET X_STS ='PENDING' WHERE X_EXP_DT <= SYSDATE+1 AND X_EXP_DT IS NOT NULL AND X_PARTY_FK = " + rowIdobject + "");
                            //stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN_XREF SET X_STS ='PENDING' WHERE X_EXP_DT <= SYSDATE+1 AND X_EXP_DT IS NOT NULL AND X_PARTY_FK = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT SET X_STS_CD ='I' WHERE X_INACT_DT <= SYSDATE+1 AND X_INACT_DT IS NOT NULL AND X_PRTY_FK = " + rowIdobject + "");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT_XREF SET X_STS_CD ='I' WHERE X_INACT_DT <= SYSDATE+1 AND X_INACT_DT IS NOT NULL AND X_PRTY_FK = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS SET ACCNT_STS ='INACTIVE' WHERE EFF_END_DT <= SYSDATE+1 AND EFF_END_DT IS NOT NULL AND PRTY_FK = " + rowIdobject + "");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS_XREF SET ACCNT_STS ='INACTIVE' WHERE EFF_END_DT <= SYSDATE+1 AND EFF_END_DT IS NOT NULL AND PRTY_FK = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID SET STS_CD ='Expired' WHERE EFF_END_DT <= SYSDATE+1 AND EFF_END_DT IS NOT NULL AND PRTY_FK = " + rowIdobject + "");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID_XREF SET STS_CD ='Expired' WHERE EFF_END_DT <= SYSDATE+1 AND EFF_END_DT IS NOT NULL AND PRTY_FK = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_RESALE_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_RESALE_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE_XREF SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_RESALE_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_RESALE_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_EXPNS_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_EXPNS_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE_XREF SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_EXPNS_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_EXPNS_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                             
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS SET ACCNT_STS ='INACTIVE' WHERE EFF_END_DT <= SYSDATE+1 AND EFF_END_DT IS NOT NULL AND PRTY_FK = " + rowIdobject + "");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS_XREF SET ACCNT_STS ='INACTIVE' WHERE EFF_END_DT <= SYSDATE+1 AND EFF_END_DT IS NOT NULL AND PRTY_FK = " + rowIdobject + "");
                            
                            log.info("End of loop 2b.");
                            
                        }
                        	
                        }*/
                        if(EndDate != null)
                        {
                        	log.info("When Status and End Date is NOT NULL.");
                        	log.info("End Date - " + EndDate);
                        	
                    		stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                    		stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP_XREF SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                    		
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT SET X_INACT_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_INACT_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT_XREF SET X_INACT_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_INACT_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS_XREF SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN SET X_EXP_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PARTY_FK =" + rowIdobject + "AND X_EXP_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN_XREF SET X_EXP_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PARTY_FK =" + rowIdobject + "AND X_EXP_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS_XREF SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID_XREF SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_NM_XREF SET EFF_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EFF_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_INACTIV_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" )WHERE ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ") AND X_INACTIV_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_INACTIV_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+ rowIdobject +" )WHERE ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ") AND X_INACTIV_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_OFFSYS_NM SET X_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_OFFSYS_NM_XREF SET X_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP_XREF SET X_SUPP_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_SUPP_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT = " +rowIdobject+" ) WHERE X_PMNT_PRTY_FK = '" + rowIdobject + "' AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT = "+rowIdobject+" ) WHERE X_PMNT_PRTY_FK = '" + rowIdobject + "' AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_CNCT SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT = "+rowIdobject+" ) WHERE X_PMNT_CNCT_PRTY_FK = '" + rowIdobject + "' AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_CNCT_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT = "+rowIdobject+" ) WHERE X_PMNT_CNCT_PRTY_FK = '" + rowIdobject + "' AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_CNCT_ACCU_TECH SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT = "+rowIdobject+" ) WHERE X_PMNT_CNT_ACCUTCH_PRTY_FK = '" + rowIdobject + "' AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_CNCT_ACCU_TECH_XREF SET X_END_DATE =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT = "+rowIdobject+" ) WHERE X_PMNT_CNT_ACCUTCH_PRTY_FK = '" + rowIdobject + "' AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS_XREF SET EXPRY_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE PRTY_FK =" + rowIdobject + "AND EXPRY_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH SET X_END_DATE = (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH_XREF SET X_END_DATE = (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_PRTY_FK =" + rowIdobject + "AND X_END_DATE IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE SET X_SUPP_SUB_ROLE_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_RESALE_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_RESALE_SUPP WHERE X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE_XREF SET X_SUPP_SUB_ROLE_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_RESALE_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_RESALE_SUPP WHERE X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_END_DT IS NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE SET X_SUPP_SUB_ROLE_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_EXPNS_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_EXPNS_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_END_DT IS NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE_XREF SET X_SUPP_SUB_ROLE_END_DT =  (SELECT X_INACTV_DT  FROM SUPPLIER_HUB.C_BO_PRTY WHERE ROWID_OBJECT ="+rowIdobject+" ) WHERE X_EXPNS_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_EXPNS_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_END_DT IS NULL");
                             
                         log.info("End Date updated in child records.");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_PRTY_STS ='Inactive' WHERE X_INACTV_DT <= SYSDATE +1 AND X_INACTV_DT IS NOT NULL AND ROWID_OBJECT = '" + rowIdobject + "' AND X_PRTY_STS = 'Approved'");
                            //stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET X_PRTY_STS ='Inactive' WHERE X_INACTV_DT <= SYSDATE +1 AND X_INACTV_DT IS NOT NULL AND ROWID_OBJECT = '" + rowIdobject + "'");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP SET X_SUPP_STS ='Inactive' WHERE X_SUPP_END_DT <= SYSDATE+1 AND X_SUPP_END_DT IS NOT NULL AND X_PRTY_FK = " + rowIdobject + "");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_RESALE_SUPP_XREF SET X_SUPP_STS ='Inactive' WHERE X_SUPP_END_DT <= SYSDATE+1 AND X_SUPP_END_DT IS NOT NULL AND X_PRTY_FK = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP SET X_SUPP_STS ='Inactive' WHERE X_SUPP_END_DT <= SYSDATE+1 AND X_SUPP_END_DT IS NOT NULL AND X_PRTY_FK = " + rowIdobject + "");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_EXPNS_SUPP_XREF SET X_SUPP_STS ='Inactive' WHERE X_SUPP_END_DT <= SYSDATE+1 AND X_SUPP_END_DT IS NOT NULL AND X_PRTY_FK = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_STS ='I' WHERE X_INACTIV_DT <= SYSDATE+1 AND X_INACTIV_DT IS NOT NULL AND ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ")");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_STS ='I' WHERE X_INACTIV_DT <= SYSDATE+1 AND X_INACTIV_DT IS NOT NULL AND ROWID_OBJECT IN (SELECT PSTL_ADDR_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PRTY_FK= " + rowIdobject + ")");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN SET X_STS ='PENDING' WHERE X_EXP_DT <= SYSDATE+1 AND X_EXP_DT IS NOT NULL AND X_PARTY_FK = " + rowIdobject + "");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BUS_CLSFN_XREF SET X_STS ='PENDING' WHERE X_EXP_DT <= SYSDATE+1 AND X_EXP_DT IS NOT NULL AND X_PARTY_FK = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT SET X_STS_CD ='I' WHERE X_INACT_DT <= SYSDATE+1 AND X_INACT_DT IS NOT NULL AND X_PRTY_FK = " + rowIdobject + "");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT_XREF SET X_STS_CD ='I' WHERE X_INACT_DT <= SYSDATE+1 AND X_INACT_DT IS NOT NULL AND X_PRTY_FK = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS SET ACCNT_STS ='INACTIVE' WHERE EFF_END_DT <= SYSDATE+1 AND EFF_END_DT IS NOT NULL AND PRTY_FK = " + rowIdobject + "");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS_XREF SET ACCNT_STS ='INACTIVE' WHERE EFF_END_DT <= SYSDATE+1 AND EFF_END_DT IS NOT NULL AND PRTY_FK = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID SET STS_CD ='Expired' WHERE EFF_END_DT <= SYSDATE+1 AND EFF_END_DT IS NOT NULL AND PRTY_FK = " + rowIdobject + "");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_ALT_ID_XREF SET STS_CD ='Expired' WHERE EFF_END_DT <= SYSDATE+1 AND EFF_END_DT IS NOT NULL AND PRTY_FK = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_RESALE_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_RESALE_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE_XREF SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_RESALE_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_RESALE_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_EXPNS_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_EXPNS_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_SUB_ROLE_XREF SET X_SUPP_SUB_ROLE_STS ='Inactive', X_SUPP_SUB_ROLE_END_DT = SYSDATE WHERE X_EXPNS_SUPP_FK IN (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_EXPNS_SUPP WHERE  X_PRTY_FK = " + rowIdobject + ") AND X_SUPP_SUB_ROLE_STS IS NOT NULL");
                             
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS SET ACCNT_STS ='INACTIVE' WHERE EFF_END_DT <= SYSDATE+1 AND EFF_END_DT IS NOT NULL AND PRTY_FK = " + rowIdobject + "");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS_XREF SET ACCNT_STS ='INACTIVE' WHERE EFF_END_DT <= SYSDATE+1 AND EFF_END_DT IS NOT NULL AND PRTY_FK = " + rowIdobject + "");
                            
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XR_BNK_ADDR_REL SET X_ADDR_STS ='I' WHERE X_PRTY_BNK_FK = (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS WHERE PRTY_FK = "  + rowIdobject + " )");
                            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XR_BNK_ADDR_REL_XREF SET X_ADDR_STS ='I' WHERE X_PRTY_BNK_FK = (SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_PRTY_BNK_DTLS WHERE PRTY_FK = "  + rowIdobject + " )");
                            
                         log.info("Status updated in child records.");
                        }
                    }
                    else
                    {
                    	log.info("Error in EndDate and Party Status loops.");
                    }
    			}
               
               stmt2.close();
               con2.close();
               stmt1.close();
               con1.close();
               stmt.close();
	           con.close(); 
              } 
			catch (Exception e) 
			{
				log.info("Exception in C_BO_PRTY DB connection creation " + e);
			}
			finally{
				try { stmt2.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt1 " + e); }
				try { con2.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con1 " + e); }
				try { stmt1.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt1 " + e); }
				try { con1.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con1 " + e); }
				try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
			}
		}
//--------------------------------------------------------------C_BO_PSTL_ADDR----------------------------------------------------------------------------------------
				log.info("Line 255 TableName: " + tablename);
				// Generating objectId for C_BO_PRTY
				if (tablename != null && tablename.equals("C_BO_PSTL_ADDR")) {
					Set<String> setBOKey = baseObjectDataMap.keySet();
					if (setBOKey != null) {
						Iterator<String> itrBOKey = setBOKey.iterator();
						if (itrBOKey != null) {
							while (itrBOKey.hasNext()) {
								String stBOKey = (String) itrBOKey.next();
								Object objBO = (Object) baseObjectDataMap.get(stBOKey);
								if (objBO instanceof String) {
									String stValue = (String) objBO;
									if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
										rowIdobject = stValue;
										rowIdobject = rowIdobject.trim();
										log.info("C_BO_PSTL_ADDR_LOAD rowIdObject " + rowIdobject);
									}
								}
							}
						}
					}
					log.info("Begin DB C_BO_PRTY Connection creation 2369");
					Context ctx = null;
					Connection con = null;
					Statement stmt = null;
					Connection con1 = null;
					Statement stmt1 = null;
					Statement stmtfk = null;
					try {
						ctx = new InitialContext();
						DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
						con = ds.getConnection();
						stmt = con.createStatement();
						con1 = ds.getConnection();
						stmt1 = con1.createStatement();
						stmtfk = con1.createStatement();
						
						ResultSet rsFKDBA = stmtfk.executeQuery("SELECT C.ROWID_OBJECT AS PRTY_ID FROM SUPPLIER_HUB.C_BO_PSTL_ADDR A"
								+ " LEFT JOIN SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR B ON A.ROWID_OBJECT = B.PSTL_ADDR_FK"
								+ " LEFT JOIN SUPPLIER_HUB.C_BO_PRTY C ON C.ROWID_OBJECT = B.PRTY_FK"
								+ " WHERE A.ROWID_OBJECT = '" + rowIdobject + "'");
						//log.info("PRTY_ID " + rsFKDBA);
						String PRTY_ID = null;
						while (rsFKDBA.next()) {
							PRTY_ID = rsFKDBA.getString("PRTY_ID");
							log.info("PRTY_ID " + PRTY_ID);
						}
						
						ResultSet mainT = stmt1.executeQuery("SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_SUPP_MAINTNC_DTLS WHERE X_PRTY_FK  = '" + PRTY_ID + "'");
						String rowid = null;
						while (mainT.next()) {
							log.info("ROWID_OBJECT_MAINT " + mainT.getString("ROWID_OBJECT"));
							rowid = mainT.getString("ROWID_OBJECT");
						}
						
						if(rowid != null) {
							log.info("Supplier IN Maintenance");
						
						ResultSet MainTAddLn1 = stmt1.executeQuery("SELECT DISTINCT cxsmd.X_RQST_STS AS X_RQST_STS_LN_1 FROM supplier_hub.c_bo_prty prty " +
								"JOIN SUPPLIER_HUB.C_XO_SUPP_MAINTNC_DTLS cxsmd ON prty.ROWID_OBJECT=cxsmd.X_PRTY_FK " +
								"JOIN SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR cbppa ON cbppa.PRTY_FK =prty.ROWID_OBJECT " +
								"JOIN SUPPLIER_HUB.C_BO_PSTL_ADDR cbpa ON cbpa.ROWID_OBJECT=cbppa.PSTL_ADDR_FK " + 
								"JOIN supplier_hub.C_BO_PSTL_ADDR_XREF xrf " +
								"ON xrf.rowid_object=cbpa.ROWID_OBJECT WHERE xrf.HUB_STATE_IND=0 AND xrf.ROWID_SYSTEM='SYS0' " +
								"AND xrf.ADDR_LN_1 <>cbpa.ADDR_LN_1 AND cbpa.ROWID_OBJECT = '" + rowIdobject + "'");
						String rqstStsAddrLn1 = null;
						log.info("Before while - MainTAddLn1");
						while (MainTAddLn1.next()) {
	                    log.info("We are in While loop. MainTAddLn1");
	                    rqstStsAddrLn1 = MainTAddLn1.getString("X_RQST_STS_LN_1");
	                    log.info("X_RQST_STS -  " + rqstStsAddrLn1);
						}
						
						ResultSet MainTAddLn2 = stmt1.executeQuery("SELECT DISTINCT cxsmd.X_RQST_STS AS X_RQST_STS_LN_2 FROM supplier_hub.c_bo_prty prty " +
								"JOIN SUPPLIER_HUB.C_XO_SUPP_MAINTNC_DTLS cxsmd ON prty.ROWID_OBJECT=cxsmd.X_PRTY_FK " +
								"JOIN SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR cbppa ON cbppa.PRTY_FK =prty.ROWID_OBJECT " +
								"JOIN SUPPLIER_HUB.C_BO_PSTL_ADDR cbpa ON cbpa.ROWID_OBJECT=cbppa.PSTL_ADDR_FK " + 
								"JOIN supplier_hub.C_BO_PSTL_ADDR_XREF xrf " +
								"ON xrf.rowid_object=cbpa.ROWID_OBJECT WHERE xrf.HUB_STATE_IND=0 AND xrf.ROWID_SYSTEM='SYS0' " +
								"AND xrf.ADDR_LN_2 <>cbpa.ADDR_LN_2 AND cbpa.ROWID_OBJECT = '" + rowIdobject + "'");
						String rqstStsAddrLn2 = null;
						log.info("Before while - MainTAddLn2");
						while (MainTAddLn2.next()) {
		                log.info("We are in While loop. MainTAddLn2");
		                rqstStsAddrLn2 = MainTAddLn2.getString("X_RQST_STS_LN_2");
		                log.info("X_RQST_STS -  " + rqstStsAddrLn2);
						}
						
						ResultSet MainTCity = stmt1.executeQuery("SELECT DISTINCT cxsmd.X_RQST_STS AS X_RQST_STS_CITY FROM supplier_hub.c_bo_prty prty " +
								"JOIN SUPPLIER_HUB.C_XO_SUPP_MAINTNC_DTLS cxsmd ON prty.ROWID_OBJECT=cxsmd.X_PRTY_FK " +
								"JOIN SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR cbppa ON cbppa.PRTY_FK =prty.ROWID_OBJECT " +
								"JOIN SUPPLIER_HUB.C_BO_PSTL_ADDR cbpa ON cbpa.ROWID_OBJECT=cbppa.PSTL_ADDR_FK " + 
								"JOIN supplier_hub.C_BO_PSTL_ADDR_XREF xrf " +
								"ON xrf.rowid_object=cbpa.ROWID_OBJECT WHERE xrf.HUB_STATE_IND=0 AND xrf.ROWID_SYSTEM='SYS0' " +
								"AND xrf.CITY <>cbpa.CITY AND cbpa.ROWID_OBJECT = '" + rowIdobject + "'");
						String rqstStsCity = null;
						log.info("Before while - MainTCity");
						while (MainTCity.next()) {
		                log.info("We are in While loop. MainTCity");
		                rqstStsCity = MainTCity.getString("X_RQST_STS_LN_1");
		                log.info("X_RQST_STS_CITY -  " + rqstStsCity);
						}
						
						ResultSet MainTState = stmt1.executeQuery("SELECT DISTINCT cxsmd.X_RQST_STS AS X_RQST_STS_X_STATE FROM supplier_hub.c_bo_prty prty " +
								"JOIN SUPPLIER_HUB.C_XO_SUPP_MAINTNC_DTLS cxsmd ON prty.ROWID_OBJECT=cxsmd.X_PRTY_FK " +
								"JOIN SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR cbppa ON cbppa.PRTY_FK =prty.ROWID_OBJECT " +
								"JOIN SUPPLIER_HUB.C_BO_PSTL_ADDR cbpa ON cbpa.ROWID_OBJECT=cbppa.PSTL_ADDR_FK " + 
								"JOIN supplier_hub.C_BO_PSTL_ADDR_XREF xrf " +
								"ON xrf.rowid_object=cbpa.ROWID_OBJECT WHERE xrf.HUB_STATE_IND=0 AND xrf.ROWID_SYSTEM='SYS0' " +
								"AND xrf.X_STATE <>cbpa.X_STATE AND cbpa.ROWID_OBJECT = '" + rowIdobject + "'");
						String rqstStsState = null;
						log.info("Before while - MainTState");
					    while (MainTState.next()) {
			            log.info("We are in While loop. MainTState");
			            rqstStsState = MainTState.getString("X_RQST_STS_LN_2");
			            log.info("X_RQST_STS -  " + rqstStsState);
					    }
						
						ResultSet MainTAddrNm = stmt1.executeQuery("SELECT DISTINCT cxsmd.X_RQST_STS AS X_RQST_STS_X_ADDR_NM FROM supplier_hub.c_bo_prty prty " +
								"JOIN SUPPLIER_HUB.C_XO_SUPP_MAINTNC_DTLS cxsmd ON prty.ROWID_OBJECT=cxsmd.X_PRTY_FK " +
								"JOIN SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR cbppa ON cbppa.PRTY_FK =prty.ROWID_OBJECT " +
								"JOIN SUPPLIER_HUB.C_BO_PSTL_ADDR cbpa ON cbpa.ROWID_OBJECT=cbppa.PSTL_ADDR_FK " + 
								"JOIN supplier_hub.C_BO_PSTL_ADDR_XREF xrf " +
								"ON xrf.rowid_object=cbpa.ROWID_OBJECT WHERE xrf.HUB_STATE_IND=0 AND xrf.ROWID_SYSTEM='SYS0' " +
								"AND xrf.X_ADDR_NM <>cbpa.X_ADDR_NM AND cbpa.ROWID_OBJECT = '" + rowIdobject + "'");
						String rqstStsAddrNm = null;
						 log.info("Before while - MainTAddrNm");
						    while (MainTAddrNm.next()) {
				            log.info("We are in While loop. MainTAddrNm");
				            rqstStsAddrNm = MainTAddrNm.getString("X_RQST_STS_LN_1");
				            log.info("X_RQST_STS -  " + rqstStsAddrNm);
						    }
						
						ResultSet MainTPayeeNm = stmt1.executeQuery("SELECT DISTINCT cxsmd.X_RQST_STS AS X_RQST_STS_X_PAYEE_NM FROM supplier_hub.c_bo_prty prty " +
								"JOIN SUPPLIER_HUB.C_XO_SUPP_MAINTNC_DTLS cxsmd ON prty.ROWID_OBJECT=cxsmd.X_PRTY_FK " +
								"JOIN SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR cbppa ON cbppa.PRTY_FK =prty.ROWID_OBJECT " +
								"JOIN SUPPLIER_HUB.C_BO_PSTL_ADDR cbpa ON cbpa.ROWID_OBJECT=cbppa.PSTL_ADDR_FK " + 
								"JOIN supplier_hub.C_BO_PSTL_ADDR_XREF xrf " +
								"ON xrf.rowid_object=cbpa.ROWID_OBJECT WHERE xrf.HUB_STATE_IND=0 AND xrf.ROWID_SYSTEM='SYS0' " +
								"AND xrf.X_PAYEE_NM <>cbpa.X_PAYEE_NM AND cbpa.ROWID_OBJECT = '" + rowIdobject + "'");
						String rqstStsPayeeNm = null;
						log.info("Before while - MainTPayeeNm");
						while (MainTPayeeNm.next()) {
				        log.info("We are in While loop. MainTPayeeNm");
				        rqstStsPayeeNm = MainTPayeeNm.getString("X_RQST_STS_LN_2");
				        log.info("X_RQST_STS -  " + rqstStsPayeeNm);
						}

						if(rqstStsAddrLn1 != null || rqstStsAddrLn2 != null || rqstStsCity != null || rqstStsState != null || rqstStsAddrNm != null || rqstStsPayeeNm != null) {
						
						ResultSet normAddr = stmt1.executeQuery("SELECT ADDR_LN_1, ADDR_LN_2, CITY, X_STATE, X_ADDR_NM, X_PAYEE_NM FROM SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF WHERE ROWID_OBJECT = '" + rowIdobject + "'");
						String AddLn1x = null;
						String AddLn2x = null;
						String cityx = null;
						String x_statex = null;
						String AddNmx = null;
						String PayeeNmx = null;
						while (normAddr.next()) {
		                    log.info("We are in While loop.");
		                    AddLn1x = normAddr.getString("ADDR_LN_1");
		                    log.info("ADDR_LN_1 -  " + AddLn1x);
		                    AddLn2x = normAddr.getString("ADDR_LN_2");
		                    log.info("ADDR_LN_2 -  " + AddLn2x);
		                    cityx = normAddr.getString("CITY");
		                    log.info("CITY -  " + cityx);
		                    x_statex = normAddr.getString("X_STATE");
		                    log.info("X_STATE -  " + x_statex);
		                    AddNmx = normAddr.getString("X_ADDR_NM");
		                    log.info("X_ADDR_NM -  " + AddNmx);
		                    PayeeNmx = normAddr.getString("X_PAYEE_NM");
		                    log.info("X_PAYEE_NM -  " + PayeeNmx);
		                }
						
						String ADDR_LN_1_x = ( Normalizer.normalize(AddLn1x.toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
						log.info("Normalized AddrLn1 : "+ ADDR_LN_1_x);
						//stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET ADDR_LN_1 = '" + ADDR_LN_1_x + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					    stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET ADDR_LN_1 = '" + ADDR_LN_1_x + "' WHERE ROWID_OBJECT =" + rowIdobject + " AND HUB_STATE_IND = 0");
					    
					    String ADDR_LN_2_x = ( Normalizer.normalize(AddLn2x.toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
						log.info("Normalized AddrLn2 : "+ ADDR_LN_2_x);
						if (ADDR_LN_2_x != null) {
						//stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET ADDR_LN_2 = '" + ADDR_LN_2_x + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					    stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET ADDR_LN_2 = '" + ADDR_LN_2_x + "' WHERE ROWID_OBJECT =" + rowIdobject + " AND HUB_STATE_IND = 0");
						}
					    
					    String CITY_x = ( Normalizer.normalize(cityx.toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
						log.info("Normalized CITY : "+ CITY_x);
						//stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET CITY = '" + CITY_x + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					    stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET CITY = '" + CITY_x + "' WHERE ROWID_OBJECT =" + rowIdobject + " AND HUB_STATE_IND = 0");
					    
					    String STATE_x = ( Normalizer.normalize(x_statex.toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
						log.info("Normalized AddrLn1 : "+ STATE_x);
						//stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_STATE = '" + STATE_x + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					    stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_STATE = '" + STATE_x + "' WHERE ROWID_OBJECT =" + rowIdobject + " AND HUB_STATE_IND = 0");
					    
					    String ADDR_NM_x = ( Normalizer.normalize(AddNmx.toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
						log.info("Normalized AddrLn1 : "+ ADDR_NM_x);
						//stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_ADDR_NM = '" + ADDR_NM_x + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					    stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_ADDR_NM = '" + ADDR_NM_x + "' WHERE ROWID_OBJECT =" + rowIdobject + " AND HUB_STATE_IND = 0");
					    
					    String PAYEE_NM_x = ( Normalizer.normalize(PayeeNmx.toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
						log.info("Normalized AddrLn1 : "+ PAYEE_NM_x);
						//stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_PAYEE_NM = '" + PAYEE_NM_x + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
					    stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_PAYEE_NM = '" + PAYEE_NM_x + "' WHERE ROWID_OBJECT =" + rowIdobject + " AND HUB_STATE_IND = 0");
					    
						
					}
				}
						
					else {
						
						log.info("Supplier NOT IN Maintenance");
						
						if ((baseObjectDataMap.get("ADDR_LN_1")) != null) {
						  String ADDR_LN_1 = (Normalizer.normalize(baseObjectDataMap.get("ADDR_LN_1").toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
						  log.info("Normalized ADDR_LN_1 : " + ADDR_LN_1);
						  if (ADDR_LN_1 != null) {
						    stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET ADDR_LN_1 = '" + ADDR_LN_1 + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						    stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET ADDR_LN_1 = '" + ADDR_LN_1 + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						  }
						  log.info("Successfully updated : " + " " + ADDR_LN_1);
						}

						if ((baseObjectDataMap.get("ADDR_LN_2")) != null) {
						  String ADDR_LN_2 = (Normalizer.normalize(baseObjectDataMap.get("ADDR_LN_2").toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
						  log.info("Normalized ADDR_LN_2 : " + ADDR_LN_2);
						  if (ADDR_LN_2 != null) {
						  stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET ADDR_LN_2 = '" + ADDR_LN_2 + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						  stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET ADDR_LN_2 = '" + ADDR_LN_2 + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						  log.info("Successfully updated : " + " " + ADDR_LN_2);
						}
						}
						if ((baseObjectDataMap.get("CITY")) != null) {
						  String CITY = (Normalizer.normalize(baseObjectDataMap.get("CITY").toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
						  log.info("Normalized CITY : " + CITY);
						  stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET CITY = '" + CITY + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						  stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET CITY = '" + CITY + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						  log.info("Successfully updated : " + " " + CITY);
						}
						//}
						if ((baseObjectDataMap.get("X_STATE")) != null) {
						  String X_STATE = (Normalizer.normalize(baseObjectDataMap.get("X_STATE").toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
						  log.info("Normalized X_STATE : " + X_STATE);

						  stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_STATE = '" + X_STATE + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						  stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_STATE = '" + X_STATE + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						  log.info("Successfully updated : " + " " + X_STATE);
						}
						//}
						if ((baseObjectDataMap.get("X_ADDR_NM")) != null) {
						  String X_ADDR_NM = (Normalizer.normalize(baseObjectDataMap.get("X_ADDR_NM").toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
						  log.info("Normalized X_ADDR_NM : " + X_ADDR_NM);

						  stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_ADDR_NM = '" + X_ADDR_NM + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						  stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_ADDR_NM = '" + X_ADDR_NM + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						  log.info("Successfully updated : " + " " + X_ADDR_NM);
						}
						//}
						if ((baseObjectDataMap.get("X_PAYEE_NM")) != null) {
						  String X_PAYEE_NM = (Normalizer.normalize(baseObjectDataMap.get("X_PAYEE_NM").toString(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", ""));
						  log.info("Normalized X_PAYEE_NM : " + X_PAYEE_NM);
						  
						  stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_PAYEE_NM = '" + X_PAYEE_NM + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						  stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_PAYEE_NM = '" + X_PAYEE_NM + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						  log.info("Successfully updated : " + " " + X_PAYEE_NM);
						}
					}
						stmtfk.close();
						stmt1.close();
						con1.close();
						stmt.close();
						con.close();
					} catch (Exception e) {
						log.info("Exception in C_BO_PRTY DB connection creation " + e);
					}
					finally{
						try { stmtfk.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
						try { stmt1.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
						try { con1.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
						try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
						try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }
					}
				}

//-----------------------------------------------------C_BO_PRTY---------------------------------------------------------------------------
		log.info("To add DBA name " + tablename);
		// Generating objectId for C_BO_PRTY
		if (tablename != null && tablename.equals("C_BO_PRTY")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_BO_PRTY_POST_LOAD rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("PRTY_FK")) {
								prtyRowIdobject = stValue;
								prtyRowIdobject = prtyRowIdobject.trim();
								log.info("C_BO_PRTY_NM_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
							}
						}
					}
				}
			}
			log.info("Begin DB C_BO_PRTY Connection creation ");
			Context ctx = null;
			Connection con = null;
			Connection con1 = null;
			Statement stmt = null;
			Statement stmt1 = null;
			try {

				ctx = new InitialContext();
				DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
				con = ds.getConnection();
				con1 = ds.getConnection();
				log.info("Connection Line 693- " + con);
				stmt = con.createStatement();
				stmt1 = con1.createStatement();
				// Generate unique Supplier Number
				GenerateSuppNumId dba = new GenerateSuppNumId();
				dba.GenerateUniqueSuppNumId(rowIdobject, stmt, stmt1, prtyRowIdobject, tablename);
				// Soft delete for DNB Check
				stmt.close();
				con.close();
				con1.close();
				stmt1.close();

			} catch (Exception e) {
				log.info("Exception in C_BO_PRTY DB connection creation " + e);
			}
			finally{
				try { stmt1.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { con1.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
			}
		}

//--------------------------------------------------------C_BO_PRTY_NM----------------------------------------------------------------------
		log.info("To add DBA name " + tablename);
		// Generating objectId for C_BO_PRTY_NM
		if (tablename != null && tablename.equals("C_BO_PRTY_NM")) {
			
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_BO_PRTY_NM_POST_LOAD rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("PRTY_FK")) {
								prtyRowIdobject = stValue;
								prtyRowIdobject = prtyRowIdobject.trim();
								log.info("C_BO_PRTY_NM_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
							}
						}
					}
				}
			}
			log.info("Begin DB C_BO_PRTY_NM Connection creation ");
			Context ctx = null;
			Connection con = null;
			Statement stmt = null;
			Statement stmt2 = null;
			Statement stmt3 = null;
			Statement stmtfk = null;
			try {

				ctx = new InitialContext();
				DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
				con = ds.getConnection();
				stmt = con.createStatement();
				stmt2 = con.createStatement();
				stmt3 = con.createStatement();
				stmtfk = con.createStatement();
				// Generate unique DBA_ALT_ID

				ResultSet rsFKDBA = null;
				String XFULL_NM = "";
				log.info("Before C_BO_PSTL_ADDR Select ");
				String sql2 = "SELECT FULL_NM AS \"XFULL_NM\" FROM SUPPLIER_HUB.C_BO_PRTY" + " WHERE ROWID_OBJECT ="
						+ prtyRowIdobject + "";
				log.info("Inside sql2" + sql2);
				rsFKDBA = stmtfk.executeQuery("SELECT FULL_NM AS \"XFULL_NM\" FROM SUPPLIER_HUB.C_BO_PRTY"
						+ " WHERE ROWID_OBJECT =" + prtyRowIdobject + "");
				while (rsFKDBA.next()) {
					log.info("XFULL_NM " + rsFKDBA.getString("XFULL_NM"));
					XFULL_NM = rsFKDBA.getString("XFULL_NM");
				}

				GenerateDBAaltID dba = new GenerateDBAaltID();
				log.info("InsideNewCall GenerateUniqueDBAaltID ");
				dba.GenerateUniqueDBAaltID(rowIdobject, stmt, prtyRowIdobject);
				log.info("InsideNewCall PutCallForDBALookup ");
				dba.PutCallForDBALookup(rowIdobject, stmt, stmt2, stmt3, tablename);
				log.info("InsideNewCall PutCallForParentLookup ");
				dba.PutCallForParentLookup(prtyRowIdobject, stmt, stmt2, stmt3, tablename, XFULL_NM);
				
				stmtfk.close();
				stmt3.close();
	            stmt2.close(); 
				stmt.close();
	            con.close();
	            
			} catch (Exception e) {
				log.info("Exception in C_BO_PRTY_NM DB connection creation " + e);
			}
			finally{
				try { stmtfk.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmtfk " + e); }
				try { stmt3.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt3 " + e); }
				try { stmt2.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt2 " + e); }
				try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
			}
			
		}

//-----------------------------------------------------C_BO_PRTY_NM------------------------------------------
		log.info("To add DBA name " + tablename);
		if (tablename != null && tablename.equals("C_BO_PRTY_NM")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_BO_PRTY_NM_POST_LOAD rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("PRTY_FK")) {
								prtyRowIdobject = stValue;
								prtyRowIdobject = prtyRowIdobject.trim();
								log.info("C_BO_PRTY_NM_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
							}
						}
					}
				}
			}
			log.info("Begin DB C_BO_PRTY_NM Connection creation ");
			Context ctx = null;
			Connection con = null;
			Statement stmt = null;
			try {
		
				ctx = new InitialContext();
				DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
				con = ds.getConnection();
				stmt = con.createStatement();
				//Insert and Update DBA Name ID
				DbaNmId dba = new DbaNmId();
				dba.DbaNameMethod(rowIdobject, stmt, tablename);
				
				// Soft delete for DNB Check
				log.info("End date of C_BO_PRTY_NM " + baseObjectDataMap.get("EFF_END_DT"));
				if(baseObjectDataMap.get("EFF_END_DT") != null) {
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_DDP_DBA_NM SET HUB_STATE_IND = -1"
						+" WHERE X_PRTY_DBA_NM_ID IN (SELECT X_ALT_NM_ID FROM SUPPLIER_HUB.C_BO_PRTY_NM WHERE ROWID_OBJECT ='"
						+ rowIdobject + "' AND EFF_END_DT < SYSDATE )");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_DDP_DBA_NM_XREF SET HUB_STATE_IND = -1"
						+" WHERE X_PRTY_DBA_NM_ID IN (SELECT X_ALT_NM_ID FROM SUPPLIER_HUB.C_BO_PRTY_NM WHERE ROWID_OBJECT ='"
						+ rowIdobject + "' AND EFF_END_DT < SYSDATE )");
				
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_DDP_DBA_NM SET HUB_STATE_IND = 1"
						+" WHERE X_PRTY_DBA_NM_ID IN (SELECT X_ALT_NM_ID FROM SUPPLIER_HUB.C_BO_PRTY_NM WHERE ROWID_OBJECT ='"
						+ rowIdobject + "' AND EFF_END_DT > SYSDATE )");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_DDP_DBA_NM_XREF SET HUB_STATE_IND = 1"
						+" WHERE X_PRTY_DBA_NM_ID IN (SELECT X_ALT_NM_ID FROM SUPPLIER_HUB.C_BO_PRTY_NM WHERE ROWID_OBJECT ='"
						+ rowIdobject + "' AND EFF_END_DT > SYSDATE )");
				}
				
				else if(baseObjectDataMap.get("EFF_END_DT") == null) {
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_DDP_DBA_NM SET HUB_STATE_IND = 1"
								+ "WHERE X_PRTY_DBA_NM_ID IN "
								+ "(SELECT X_ALT_NM_ID FROM SUPPLIER_HUB.C_BO_PRTY_NM WHERE ROWID_OBJECT ='" +rowIdobject 
								+"'AND EFF_END_DT IS NULL)");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_DDP_DBA_NM_XREF SET HUB_STATE_IND = 1"
						+ "WHERE X_PRTY_DBA_NM_ID IN "
						+ "(SELECT X_ALT_NM_ID FROM SUPPLIER_HUB.C_BO_PRTY_NM WHERE ROWID_OBJECT ='" +rowIdobject 
						+"'AND EFF_END_DT IS NULL)");
				}
				 
				stmt.close();
	            con.close();
		
			} catch (Exception e) {
				log.info("Exception in C_BO_PRTY_NM DB connection creation " + e);
			}
			finally{
				try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
			}
		}

//----------------------------------------------------------C_XO_PRTY_PMNT_DTLS-------------------------------------------------------------
		log.info("To add DBA name " + tablename);
		// Generating objectId for C_XO_PRTY_PMNT_DTLS
		if (tablename != null && tablename.equals("C_XO_PRTY_PMNT_DTLS")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_XO_PRTY_PMNT_DTLS_POST_LOAD rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
								prtyRowIdobject = stValue;
								prtyRowIdobject = prtyRowIdobject.trim();
								log.info("C_XO_PRTY_PMNT_DTLS_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
							}
						}
					}
				}
			}
			log.info("Begin DB C_XO_PRTY_PMNT_DTLS Connection creation ");
			//Ankit Date:06/11/2025
			DatabaseConnection dbHandler = new DatabaseConnection();
            Statement stmt = null;
            try {
                stmt = dbHandler.createConnection();
                GeneratePymntDtlsId pymntId = new GeneratePymntDtlsId();
				pymntId.GenerateUniquePmntDtlsId(rowIdobject, stmt, prtyRowIdobject); 
            } catch (Exception e) {
            	 log.info("Exception in C_XO_PRTY_PMNT_DTLS DB connection creation " + e);
            } finally {
                dbHandler.closeResources();
            }//Ankit Date:06/11/2025
		}

//------------------------------------------------------C_XO_PRTY_PMNT_DTLS-----------------------------------------------------------------------
		log.info("Dynamic Lookup -- Line 553 Party Payment Lookup logic :  " + tablename);
		if (tablename != null && tablename.equals("C_XO_PRTY_PMNT_DTLS")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_XO_PRTY_PMNT_DTLS_POST_LOAD rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
								prtyRowIdobject = stValue;
								prtyRowIdobject = prtyRowIdobject.trim();
								log.info("C_XO_PRTY_PMNT_DTLS_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
							}
						}
					}
				}
			}
			log.info("Begin DB C_XO_PRTY_PMNT_DTLS Connection creation ");
			Context ctx = null;
			Connection con = null;
			Statement stmt = null;
			Statement stmt2 = null;
			Statement stmt3 = null;
			Statement stmtfk = null;
			try {
		
			  ctx = new InitialContext();
			  DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
			  con = ds.getConnection();
			  stmt = con.createStatement();
			  stmt2 = con.createStatement();
			  stmt3 = con.createStatement();
			  stmtfk = con.createStatement();
		
			  //To get Supplier name with help of Party RowIDObject
			  ResultSet rsFKDBA = null;
			  String XFULL_NM = "";
			  
			  log.info("Before C_BO_PRTY Select ");
			  String sql2 = "SELECT FULL_NM AS \"XFULL_NM\" FROM SUPPLIER_HUB.C_BO_PRTY" + " WHERE ROWID_OBJECT =" +
			    prtyRowIdobject + "";
			  log.info("Inside sql2" + sql2);
			  rsFKDBA = stmtfk.executeQuery("SELECT FULL_NM AS \"XFULL_NM\" FROM SUPPLIER_HUB.C_BO_PRTY" +
			    " WHERE ROWID_OBJECT =" + prtyRowIdobject + "");
			  
			  while (rsFKDBA.next()) {
			    log.info("XFULL_NM " + rsFKDBA.getString("XFULL_NM"));
			    XFULL_NM = rsFKDBA.getString("XFULL_NM");
			  }
			  ResultSet rsFKDBA1 = null;
			  String XSUPP_NM = null;
			  log.info("Before C_XT_PRTY_FK Select ");
			  String sql21 = "SELECT X_SUPP_NM AS XSUPP_NM FROM SUPPLIER_HUB.C_XT_PRTY_FK  WHERE X_PRTY_FK =" +
			    prtyRowIdobject + "";
			  log.info("Inside sql2" + sql21);
			  rsFKDBA1 = stmtfk.executeQuery("SELECT X_SUPP_NM AS XSUPP_NM FROM SUPPLIER_HUB.C_XT_PRTY_FK  WHERE X_PRTY_FK =" + prtyRowIdobject + "");
			  while (rsFKDBA1.next()) {
			    log.info("XFULL_NM " + rsFKDBA1.getString("XSUPP_NM"));
			    XSUPP_NM = rsFKDBA1.getString("XSUPP_NM");
			  }
			  
			  if (prtyRowIdobject != null) {
			    log.info("Got Party ID ");
			    // Inserting in Wesco BU Supplier Account
			    PaymentDetailsID dba = new PaymentDetailsID();
			    dba.PaymentDetailMethod(rowIdobject, stmt, prtyRowIdobject, tablename);  // changed for maintenance
			  }
		
			  if (XSUPP_NM != null) {
			    log.info("Parent entry already exist");
			  } else {
			    GenerateDBAaltID dba = new GenerateDBAaltID();
			    log.info("InsideNewCall PutCallForParentLookup ");
			    dba.PutCallForParentLookup(prtyRowIdobject, stmt, stmt2, stmt3, tablename, XFULL_NM);
			  }
			  rsFKDBA1.close();
			  rsFKDBA.close();
			  
			  log.info("End date of C_XO_PRTY_PMNT_DTLS " + baseObjectDataMap.get("X_END_DATE"));
				if(baseObjectDataMap.get("X_END_DATE") != null) {
					
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_LOC_NM SET HUB_STATE_IND = 1"
							+" WHERE X_PMNT_DTLS_ID IN (SELECT X_PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS WHERE ROWID_OBJECT = '" + rowIdobject
							+ "' AND X_END_DATE > SYSDATE)");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_LOC_NM_XREF SET HUB_STATE_IND = 1"
							+" WHERE X_PMNT_DTLS_ID IN (SELECT X_PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS WHERE ROWID_OBJECT = '" + rowIdobject
							+ "' AND X_END_DATE > SYSDATE)");
					
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_LOC_NM SET HUB_STATE_IND =-1"
				+" WHERE X_PMNT_DTLS_ID IN (SELECT X_PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS WHERE ROWID_OBJECT = '" + rowIdobject
				+ "' AND X_END_DATE <= SYSDATE+1)");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_LOC_NM_XREF SET HUB_STATE_IND =-1"
						+" WHERE X_PMNT_DTLS_ID IN (SELECT X_PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS WHERE ROWID_OBJECT = '" + rowIdobject
						+ "' AND X_END_DATE <= SYSDATE+1)");
				
				
				}
				
				else if(baseObjectDataMap.get("X_END_DATE") == null) {
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_LOC_NM SET HUB_STATE_IND = 1"
						+" WHERE X_PMNT_DTLS_ID IN (SELECT X_PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS WHERE ROWID_OBJECT = '" + rowIdobject
						+ "' AND X_END_DATE IS NULL)");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_LOC_NM_XREF SET HUB_STATE_IND = 1"
						+" WHERE X_PMNT_DTLS_ID IN (SELECT X_PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XO_PRTY_PMNT_DTLS WHERE ROWID_OBJECT = '" + rowIdobject
						+ "' AND X_END_DATE IS NULL)");
				}
				
				stmtfk.close();
				stmt3.close();
	            stmt2.close(); 
				stmt.close();
	            con.close();
		
			} catch (Exception e) {
			  log.info("Exception in C_XO_PRTY_PMNT_DTLS DB connection creation " + e);
			}
			finally{
				try { stmtfk.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmtfk " + e); }
				try { stmt3.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt3 " + e); }
				try { stmt2.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt2 " + e); }
				try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
			}
			
		}
		
		//----------------------------------------------------------C_XO_PMNT_DTLS_ACCU_TECH-------------------------------------------------------------
				log.info("To add DBA name " + tablename);
				// Generating objectId for C_XO_PMNT_DTLS_ACCU_TECH
				if (tablename != null && tablename.equals("C_XO_PMNT_DTLS_ACCU_TECH")) {
					Set<String> setBOKey = baseObjectDataMap.keySet();
					if (setBOKey != null) {
						Iterator<String> itrBOKey = setBOKey.iterator();
						if (itrBOKey != null) {
							while (itrBOKey.hasNext()) {
								String stBOKey = (String) itrBOKey.next();
								Object objBO = (Object) baseObjectDataMap.get(stBOKey);
								if (objBO instanceof String) {
									String stValue = (String) objBO;
									if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
										rowIdobject = stValue;
										rowIdobject = rowIdobject.trim();
										log.info("C_XO_PMNT_DTLS_ACCU_TECH_POST_LOAD rowIdObject" + rowIdobject);
									}
									if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
										prtyRowIdobject = stValue;
										prtyRowIdobject = prtyRowIdobject.trim();
										log.info("C_XO_PMNT_DTLS_ACCU_TECH_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
									}
								}
							}
						}
					}
					log.info("Begin DB C_XO_PMNT_DTLS_ACCU_TECH Connection creation ");
					//Ankit Date:06/11/2025
					
			
					//log.info("Begin DB C_XO_PMNT_DTLS_ACCU_TECH Connection creation ");
					DatabaseConnection dbHandler = new DatabaseConnection();
		            Statement stmt = null;

		    try {
		        stmt = dbHandler.createConnection();
		        GeneratePymntDtlsIdAccuTech pymntId = new GeneratePymntDtlsIdAccuTech();
				pymntId.GenerateUniquePmntDtlsAccuTechId(rowIdobject, stmt, prtyRowIdobject);
				
		    } catch (Exception e) {
				log.info("Exception in C_XO_PMNT_DTLS_ACCU_TECH DB connection creation " + e);

		    } finally {
		        dbHandler.closeResources();
		    }
		    
//End Ankit Date:06/11/2025
				}
				
//------------------------------------------------------Dynamic C_XO_PMNT_DTLS_ACCU_TECH-----------------------------------------------------------------------
				log.info("Dynamic Lookup -- Line 553 Party Payment Lookup logic :  " + tablename);
				if (tablename != null && tablename.equals("C_XO_PMNT_DTLS_ACCU_TECH")) {
					Set<String> setBOKey = baseObjectDataMap.keySet();
					if (setBOKey != null) {
						Iterator<String> itrBOKey = setBOKey.iterator();
						if (itrBOKey != null) {
							while (itrBOKey.hasNext()) {
								String stBOKey = (String) itrBOKey.next();
								Object objBO = (Object) baseObjectDataMap.get(stBOKey);
								if (objBO instanceof String) {
									String stValue = (String) objBO;
									if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
										rowIdobject = stValue;
										rowIdobject = rowIdobject.trim();
										log.info("C_XO_PMNT_DTLS_ACCU_TECH_LOAD rowIdObject" + rowIdobject);
									}
									if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
										prtyRowIdobject = stValue;
										prtyRowIdobject = prtyRowIdobject.trim();
										log.info("C_XO_PMNT_DTLS_ACCU_TECH_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
									}
								}
							}
						}
					}
					log.info("Begin DB C_XO_PMNT_DTLS_ACCU_TECH Connection creation ");
					Context ctx = null;
					Connection con = null;
					Statement stmt = null;
					Statement stmt2 = null;
					Statement stmt3 = null;
					Statement stmtfk = null;
					try {
				
					  ctx = new InitialContext();
					  DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
					  con = ds.getConnection();
					  stmt = con.createStatement();
					  stmt2 = con.createStatement();
					  stmt3 = con.createStatement();
					  stmtfk = con.createStatement();
				
					  //To get Supplier name with help of Party RowIDObject
					  ResultSet rsFKDBA = null;
					  String XFULL_NM = "";
					  
					  log.info("Before C_BO_PRTY Select ");
					  String sql2 = "SELECT FULL_NM AS \"XFULL_NM\" FROM SUPPLIER_HUB.C_BO_PRTY" + " WHERE ROWID_OBJECT =" +
					    prtyRowIdobject + "";
					  log.info("Inside sql2" + sql2);
					  rsFKDBA = stmtfk.executeQuery("SELECT FULL_NM AS \"XFULL_NM\" FROM SUPPLIER_HUB.C_BO_PRTY" +
					    " WHERE ROWID_OBJECT =" + prtyRowIdobject + "");
					  
					  while (rsFKDBA.next()) {
					    log.info("XFULL_NM " + rsFKDBA.getString("XFULL_NM"));
					    XFULL_NM = rsFKDBA.getString("XFULL_NM");
					  }
					  ResultSet rsFKDBA1 = null;
					  String XSUPP_NM = null;
					  log.info("Before C_XT_PRTY_FK Select ");
					  String sql21 = "SELECT X_SUPP_NM AS XSUPP_NM FROM SUPPLIER_HUB.C_XT_PRTY_FK  WHERE X_PRTY_FK =" +
					    prtyRowIdobject + "";
					  log.info("Inside sql2" + sql21);
					  rsFKDBA1 = stmtfk.executeQuery("SELECT X_SUPP_NM AS XSUPP_NM FROM SUPPLIER_HUB.C_XT_PRTY_FK  WHERE X_PRTY_FK =" + prtyRowIdobject + "");
					  while (rsFKDBA1.next()) {
					    log.info("XFULL_NM " + rsFKDBA1.getString("XSUPP_NM"));
					    XSUPP_NM = rsFKDBA1.getString("XSUPP_NM");
					  }
					  
					  if (prtyRowIdobject != null) {
					    log.info("Got Party ID ");
					    // Inserting in Wesco BU Supplier Account
					    PaymentDetailsIDAccuTech dba = new PaymentDetailsIDAccuTech();
					    dba.PaymentDetailMethodAT(rowIdobject, stmt, prtyRowIdobject, tablename);  //Changes made for maintenance
					  }
					  if (XSUPP_NM != null) {
					    log.info("Parent entry already exist");
					  } else {
					    GenerateDBAaltID dba = new GenerateDBAaltID();
					    log.info("InsideNewCall PutCallForParentLookup ");
					    dba.PutCallForParentLookup(prtyRowIdobject, stmt, stmt2, stmt3, tablename, XFULL_NM);
					  }
					  rsFKDBA1.close();
					  rsFKDBA.close();
					  
					  log.info("End date of C_XO_PMNT_DTLS_ACCU_TECH " + baseObjectDataMap.get("X_END_DATE"));
						if(baseObjectDataMap.get("X_END_DATE") != null) {			
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_ID_ACCU_TECH SET HUB_STATE_IND = 1"
									+"WHERE X_PMNT_ID_ACCU_TECH IN (SELECT X_PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH WHERE ROWID_OBJECT = '" + rowIdobject
									+ "' AND X_END_DATE > SYSDATE)");
							
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_ID_ACCU_TECH_XREF SET HUB_STATE_IND = 1"
									+" WHERE X_PMNT_ID_ACCU_TECH IN (SELECT X_PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH WHERE ROWID_OBJECT = '" + rowIdobject
									+ "' AND X_END_DATE > SYSDATE)");
							
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_ID_ACCU_TECH SET HUB_STATE_IND =-1"
						+" WHERE X_PMNT_ID_ACCU_TECH IN (SELECT X_PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH WHERE ROWID_OBJECT = '" + rowIdobject
						+ "' AND X_END_DATE <= SYSDATE+1)");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_ID_ACCU_TECH_XREF SET HUB_STATE_IND =-1"
								+" WHERE X_PMNT_ID_ACCU_TECH IN (SELECT X_PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH WHERE ROWID_OBJECT = '" + rowIdobject
								+ "' AND X_END_DATE < SYSDATE+1)");
						
						
						
						}
						
						else if(baseObjectDataMap.get("X_END_DATE") == null) {
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_ID_ACCU_TECH SET HUB_STATE_IND = 1"
								+" WHERE X_PMNT_ID_ACCU_TECH IN (SELECT X_PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH WHERE ROWID_OBJECT = '" + rowIdobject
								+ "' AND X_END_DATE IS NULL)");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_ID_ACCU_TECH_XREF SET HUB_STATE_IND = 1"
								+" WHERE X_PMNT_ID_ACCU_TECH IN (SELECT X_PMNT_DTLS_ID FROM SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH WHERE ROWID_OBJECT = '" + rowIdobject
								+ "' AND X_END_DATE IS NULL)");
						}
						
						stmtfk.close();
						stmt3.close();
			            stmt2.close(); 
						stmt.close();
			            con.close(); 
				
					} catch (Exception e) {
					  log.info("Exception in C_XO_PRTY_PMNT_DTLS DB connection creation " + e);
					}
					finally{
						try { stmtfk.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmtfk " + e); }
						try { stmt3.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt3 " + e); }
						try { stmt2.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt2 " + e); }
						try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
						try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
					}
					
				}
				
//---------------------------------------------------------C_XO_PMNT_CNCT_ACCU_TECH-------------------------------------------------------------------
				log.info("To add DBA name " + tablename);
				// Generating LocId for C_XO_SUPP_ACC_DDP
				String emailAddr1 = null;
				if (tablename != null && tablename.equals("C_XO_PMNT_CNCT_ACCU_TECH")) {
					Set<String> setBOKey = baseObjectDataMap.keySet();
					if (setBOKey != null) {
						Iterator<String> itrBOKey = setBOKey.iterator();
						if (itrBOKey != null) {
							while (itrBOKey.hasNext()) {
								String stBOKey = (String) itrBOKey.next();
								Object objBO = (Object) baseObjectDataMap.get(stBOKey);
								if (objBO instanceof String) {
									String stValue = (String) objBO;
									if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
										rowIdobject = stValue;
										rowIdobject = rowIdobject.trim();
										log.info("C_XO_PRTY_PMNT_CNCT rowIdObject" + rowIdobject);
									}
									if (stBOKey != null && stBOKey.equals("X_EMAIL_ADDR")) {
										emailAddr1 = stValue;
										emailAddr1 = emailAddr1.trim();
										log.info("C_XO_PRTY_PMNT_CNCT emailAddr" + emailAddr1);
									}
								}
							}
						}
					}
					log.info("Begin DB C_XO_PMNT_CNCT_ACCU_TECH Connection creation ");
					Context ctx = null;
					Connection con = null;
					Statement stmt = null;
					try {
				
						ctx = new InitialContext();
						DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
						con = ds.getConnection();
						stmt = con.createStatement();
						

						ResultSet childOne = null;
						childOne = stmt.executeQuery("SELECT X_PRTY_FK , X_EMAIL_ADDR  FROM SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT"
								+ " WHERE X_CNCT_ROW_ID =" + emailAddr1 + "");
						String emailId1 = "";
						String prtyRowIdobject1 = "";
						while (childOne.next()) {
							log.info("X_PRTY_FK " + childOne.getString("X_PRTY_FK"));
							prtyRowIdobject1 = childOne.getString("X_PRTY_FK");
							log.info("X_EMAIL_ADDR " + childOne.getString("X_EMAIL_ADDR"));
							emailId1 = childOne.getString("X_EMAIL_ADDR");
						}
						
						AssociateContact emailLastnm = new AssociateContact();
						log.info("InsideNewCall AssociateContactPmntAccuTechChild");
						emailLastnm.AssociateContactPmntAccuTechChild(rowIdobject, prtyRowIdobject1, stmt, emailId1,emailAddr1);   
						
						stmt.close();
						con.close();
				
					} catch (Exception e) {
						log.info("Exception in C_XO_PMNT_CNCT_ACCU_TECH DB connection creation " + e);
					}
					finally{
						try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
						try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
					}
				}

//-----------------------------------------------------C_XO_SUPP_ACC_DDP--------------------------------------------------------------------
		log.info("To add DBA name " + tablename);
		// Generating objectId for C_XO_SUPP_ACC_DDP
		if (tablename != null && tablename.equals("C_XO_SUPP_ACC_DDP")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_XO_SUPP_ACC_DDP_POST_LOAD rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
								prtyRowIdobject = stValue;
								prtyRowIdobject = prtyRowIdobject.trim();
								log.info("C_XO_SUPP_ACC_DDP_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
							}
						}
					}
				}
			}
			log.info("Begin DB C_XO_SUPP_ACC_DDP Connection creation ");
			
			//Ankit Date:06-11-2025
			log.info("Begin DB C_XO_SUPP_ACC_DDP Connection creation ");
			
			DatabaseConnection dbHandler = new DatabaseConnection();
            Statement stmt = null;

    try {
        stmt = dbHandler.createConnection();
        GeneratePrchsFrmId prchsfrmId = new GeneratePrchsFrmId();
		prchsfrmId.GenerateUniquePrchsFrmId(rowIdobject, stmt);
		
    } catch (Exception e) {
    	log.info("Exception in C_XO_SUPP_ACC_DDP DB connection creation " + e); 
    } finally {
        dbHandler.closeResources();
    }	
			//Ankit Date:06-11-2025
		}

//-----------------------------------------------------C_XO_SUPP_ACC_ACCU_TECH--------------------------------------------------------------------
		log.info("To add DBA name " + tablename);
		// Generating objectId for C_XO_SUPP_ACC_DDP
		if (tablename != null && tablename.equals("C_XO_SUPP_ACC_ACCU_TECH")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_XO_SUPP_ACC_ACCU_TECH rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
								prtyRowIdobject = stValue;
								prtyRowIdobject = prtyRowIdobject.trim();
								log.info("C_XO_SUPP_ACC_ACCU_TECH Party ID rowIdObject" + prtyRowIdobject);
							}
						}
					}
				}
			}
			log.info("Begin DB C_XO_SUPP_ACC_ACCU_TECH Connection creation ");

			//Ankit:Date:06/11/2025
			
			log.info("Begin DB C_XO_SUPP_ACC_ACCU_TECH Connection creation ");
			 
            DatabaseConnection dbHandler = new DatabaseConnection();
            Statement stmt = null;

            try {
                stmt = dbHandler.createConnection();
                GeneratePrchsFrmIdAccuTech prchsfrmId = new GeneratePrchsFrmIdAccuTech();
                prchsfrmId.GenerateUniquePrchsFrmIdAccuTech(rowIdobject, stmt);
            } catch (Exception e) {
                log.info("Exception in C_XO_SUPP_ACC_ACCU_TECH DB connection creation " + e);
            } finally {
                dbHandler.closeResources();
            }

			//End Ankit:Date:06/11/2025
		}


//-----------------------------------------------------C_XO_ASSOC_PRTY_DBA_NM--------------------------------------------------------------------
				log.info("To add C_XO_ASSOC_PRTY_DBA_NM: " + tablename);
				// Generating objectId for C_XO_SUPP_ACC_DDP
				if (tablename != null && tablename.equals("C_XO_ASSOC_PRTY_DBA_NM")) {
					Set<String> setBOKey = baseObjectDataMap.keySet();
					if (setBOKey != null) {
						Iterator<String> itrBOKey = setBOKey.iterator();
						if (itrBOKey != null) {
							while (itrBOKey.hasNext()) {
								String stBOKey = (String) itrBOKey.next();
								Object objBO = (Object) baseObjectDataMap.get(stBOKey);
								if (objBO instanceof String) {
									String stValue = (String) objBO;
									if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
										rowIdobject = stValue;
										rowIdobject = rowIdobject.trim();
										log.info("C_XO_ASSOC_PRTY_DBA_NM rowIdObject" + rowIdobject);
									}
									
								}
							}
						}
					}
					
					log.info("Begin DB C_XO_ASSOC_PRTY_DBA_NM Connection creation ");
					Context ctx = null;
					Connection con = null;
					Statement stmt = null;
					try {
				
						ctx = new InitialContext();
						DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
						con = ds.getConnection();
						stmt = con.createStatement();
						
						
						log.info("UPDATE SUPPLIER_HUB.C_XO_ASSOC_PRTY_DBA_NM SET X_ASSOC_DBA_NM_ID = X_ASSOC_DBA_NM " 
								+ " WHERE ROWID_OBJECT =" + rowIdobject + "");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_ASSOC_PRTY_DBA_NM SET X_ASSOC_DBA_NM_ID = X_ASSOC_DBA_NM " 
						+ " WHERE ROWID_OBJECT =" + rowIdobject + "");
						log.info("C_XO_ASSOC_PRTY_DBA_NM : Line 1855");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_ASSOC_PRTY_DBA_NM_XREF SET X_ASSOC_DBA_NM_ID = X_ASSOC_DBA_NM " 
								+ " WHERE ROWID_OBJECT =" + rowIdobject + "");
								log.info("C_XO_ASSOC_PRTY_DBA_NM : Line 1855");
								
						
						stmt.close();
						con.close();
				
					} catch (Exception e) {
						log.info("Exception in C_XO_ASSOC_PRTY_DBA_NM DB connection creation " + e);
					}
					finally{
						try { stmt.close(); } catch (Exception e) { log.info("C_XO_ASSOC_PRTY_DBA_NM_Exception in DB connection closure stmt " + e); }
						try { con.close(); } catch (Exception e) { log.info("C_XO_ASSOC_PRTY_DBA_NM_Exception in DB connection closure con " + e); }	
					}
				}


//---------------------------------------------C_XR_SUPP_ACC_BU_REL------------------------------------------------------------------
		log.info("To add DBA name " + tablename);
		// Generating objectId for C_XR_SUPP_ACC_BU_REL
		if (tablename != null && tablename.equals("C_XR_SUPP_ACC_BU_REL")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_XR_SUPP_ACC_BU_REL_POST_LOAD rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
								prtyRowIdobject = stValue;
								prtyRowIdobject = prtyRowIdobject.trim();
								log.info("C_XR_SUPP_ACC_BU_REL_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
							}
						}
					}
				}
			}
			log.info("Begin DB C_XR_SUPP_ACC_BU_REL Connection creation ");
			Context ctx = null;
			Connection con = null;
			Statement stmt = null;
			Statement stmtfk = null;
			Statement stmt1 = null;
			Statement stmt2 = null;
			try {
		
				ctx = new InitialContext();
				DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
				con = ds.getConnection();
				stmt = con.createStatement();
				stmt1 = con.createStatement();
				stmt2 = con.createStatement();
				stmtfk = con.createStatement();
				ResultSet rsFKDBA = null;
				String accDDPRowIdobject = "";
				log.info("Before C_BO_PSTL_ADDR Select ");
				String sql2 = "SELECT A.X_PRTY_FK AS \"PRTY_FK\" FROM SUPPLIER_HUB.C_XO_SUPP_ACC_DDP A"
						+ " LEFT JOIN SUPPLIER_HUB.C_XR_SUPP_ACC_BU_REL B ON A.ROWID_OBJECT = B.X_SUPP_ACC_DDP_FK"
						+ " WHERE B.ROWID_OBJECT =" + rowIdobject + "";
				log.info("Inside sql2" + sql2);
				rsFKDBA = stmtfk.executeQuery(
						"SELECT A.X_PRTY_FK AS \"PRTY_FK\",A.ROWID_OBJECT AS \"ROWID_OBJECT\" FROM SUPPLIER_HUB.C_XO_SUPP_ACC_DDP A"
								+ " LEFT JOIN SUPPLIER_HUB.C_XR_SUPP_ACC_BU_REL B ON A.ROWID_OBJECT = B.X_SUPP_ACC_DDP_FK"
								+ " WHERE B.ROWID_OBJECT =" + rowIdobject + "");
				while (rsFKDBA.next()) {
					log.info("PRTY_FK " + rsFKDBA.getString("PRTY_FK"));
					log.info("ROWID_OBJECT " + rsFKDBA.getString("ROWID_OBJECT"));
					prtyRowIdobject = rsFKDBA.getString("PRTY_FK");
					accDDPRowIdobject = rsFKDBA.getString("ROWID_OBJECT");
				}
		
				// Generate unique DBA_ALT_ID
				GeneratePrchsFrmId prchsfrmId = new GeneratePrchsFrmId();
				prchsfrmId.GenerateUniqueBUSuppAccId(rowIdobject, prtyRowIdobject, stmt, stmt1, stmt2);
				// Soft delete for DNB Check
				stmtfk.close();
				stmt2.close();
				stmt1.close();
				stmt.close();
				con.close();
		
			} catch (Exception e) {
				log.info("Exception in C_XR_SUPP_ACC_BU_REL DB connection creation " + e);
			}
			finally{
				try { stmtfk.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { stmt2.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { stmt1.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }
				try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
			}
		}

//---------------------------------------------------C_XO_PRTY_PMNT_DTLS--------------------------------------------------------------
		log.info("To add DBA name " + tablename);
			// Generating objectId for C_XO_PRTY_PMNT_DTLS
			if (tablename != null && tablename.equals("C_XO_PRTY_PMNT_DTLS")) {
				Set<String> setBOKey = baseObjectDataMap.keySet();
				if (setBOKey != null) {
					Iterator<String> itrBOKey = setBOKey.iterator();
					if (itrBOKey != null) {
						while (itrBOKey.hasNext()) {
							String stBOKey = (String) itrBOKey.next();
							Object objBO = (Object) baseObjectDataMap.get(stBOKey);
							if (objBO instanceof String) {
								String stValue = (String) objBO;
								if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
									rowIdobject = stValue;
									rowIdobject = rowIdobject.trim();
									log.info("C_XO_PRTY_PMNT_DTLS_POST_LOAD rowIdObject" + rowIdobject);
								}
								if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
									prtyRowIdobject = stValue;
									prtyRowIdobject = prtyRowIdobject.trim();
									log.info("C_XO_PRTY_PMNT_DTLS_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
								}
							}
						}
					}
				}
				log.info("Begin DB C_XO_PRTY_PMNT_DTLS Connection creation ");
				// Ankit: Date:06/11/2025
			/*	Context ctx = null;
				Connection con = null;
				Statement stmt = null;
				try {
	
					ctx = new InitialContext();
					DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
					con = ds.getConnection();
					stmt = con.createStatement();
					// Generate unique DBA_ALT_ID
					GeneratePymntDtlsId pymntId = new GeneratePymntDtlsId();
					pymntId.GenerateUniquePmntDtlsId(rowIdobject, stmt, prtyRowIdobject);
					// Soft delete for DNB Check
					stmt.close();
					con.close();
	
				} catch (Exception e) {
					log.info("Exception in C_XO_PRTY_PMNT_DTLS DB connection creation " + e);
				}
				finally{
					try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
					try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
				}*/
				
			log.info("Begin DB C_XO_PRTY_PMNT_DTLS Connection creation ");
			DatabaseConnection dbHandler = new DatabaseConnection();
            Statement stmt = null;

            try {
                stmt = dbHandler.createConnection();
                GeneratePymntDtlsId pymntId = new GeneratePymntDtlsId();
					pymntId.GenerateUniquePmntDtlsId(rowIdobject, stmt, prtyRowIdobject);
            } catch (Exception e) {
            	log.info("Exception in C_XO_PRTY_PMNT_DTLS DB connection creation " + e);
            } finally {
                dbHandler.closeResources();
            }

				//End Ankit: Date:06/11/2025
			}

//------------------------------------------------------C_XO_SUPP_ACC_DDP---------------------------------------------------------------------
			log.info("To add DBA name " + tablename);
			// Generating objectId for C_XO_SUPP_ACC_DDP
			if (tablename != null && tablename.equals("C_XO_SUPP_ACC_DDP")) {
				Set<String> setBOKey = baseObjectDataMap.keySet();
				if (setBOKey != null) {
					Iterator<String> itrBOKey = setBOKey.iterator();
					if (itrBOKey != null) {
						while (itrBOKey.hasNext()) {
							String stBOKey = (String) itrBOKey.next();
							Object objBO = (Object) baseObjectDataMap.get(stBOKey);
							if (objBO instanceof String) {
								String stValue = (String) objBO;
								if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
									rowIdobject = stValue;
									rowIdobject = rowIdobject.trim();
									log.info("C_XO_SUPP_ACC_DDP_POST_LOAD rowIdObject" + rowIdobject);
								}
								if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
									prtyRowIdobject = stValue;
									prtyRowIdobject = prtyRowIdobject.trim();
									log.info("C_XO_SUPP_ACC_DDP_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
								}
							}
						}
					}
				}
				log.info("Begin DB C_XO_SUPP_ACC_DDP Connection creation ");
				//Ankit: Date:06/11/2025
				
				DatabaseConnection dbHandler = new DatabaseConnection();
	            Statement stmt = null;

	    try {
	        stmt = dbHandler.createConnection();
	        GeneratePrchsFrmId prchsfrmId = new GeneratePrchsFrmId();
			prchsfrmId.GenerateUniquePrchsFrmId(rowIdobject, stmt);
			
	    } catch (Exception e) {
	    	log.info("Exception in C_XO_SUPP_ACC_BU_DDP DB connection creation " + e);
	    	} finally {
	        dbHandler.closeResources();
	    }		
				//End Ankit: Date:06/11/2025
			}

//--------------------------------------------------------C_BO_PSTL_ADDR-----------------------------------------------------------------------
			log.info("To add Address name " + tablename);
			// Generating objectId for C_XR_SUPP_ACC_BU_REL
			String status = "";
			if (tablename != null && tablename.equals("C_BO_PSTL_ADDR")) {
				log.info("POST_LOADNew baseObjectDataMap " + baseObjectDataMap);
				Set<String> setBOKey = baseObjectDataMap.keySet();
				if (setBOKey != null) {
					Iterator<String> itrBOKey = setBOKey.iterator();
					if (itrBOKey != null) {
						while (itrBOKey.hasNext()) {
							String stBOKey = (String) itrBOKey.next();
							Object objBO = (Object) baseObjectDataMap.get(stBOKey);
							if (objBO instanceof String) {
								String stValue = (String) objBO;
								if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
									rowIdobject = stValue;
									rowIdobject = rowIdobject.trim();
									log.info("C_BO_PSTL_ADDR_POST_LOAD rowIdObject" + rowIdobject);
								}
								if (stBOKey != null && stBOKey.equals("X_STS")) {
									status = stValue;
									status = status.trim();
									log.info("C_BO_PSTL_ADDR_POST_LOAD X_STS" + status);
								}
							}
						}
					}
				}
			log.info("Begin DB C_BO_PSTL_ADDR Connection creation ");
			Context ctx = null;
			Connection con = null;
			Statement stmt = null;
			Statement stmtfk = null;
			Statement stmt1 = null;
			Statement stmt2 = null;
			Statement stmt3 = null;
			Statement stmt4 = null;
			Statement stmt5 = null;
			try {
	
				ctx = new InitialContext();
				DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
				con = ds.getConnection();
				stmt = con.createStatement();
				stmt1 = con.createStatement();
				stmt2 = con.createStatement();
				stmt3 = con.createStatement();
				stmt4 = con.createStatement();
				stmt5 = con.createStatement();
				stmtfk = con.createStatement();
				ResultSet rsFKDBA = null;
				ResultSet Child = null;
				String PRTY_ID = "";
				
	
				log.info("Before C_BO_PSTL_ADDR Select ");
				
				String sql2 =
						"SELECT C.ROWID_OBJECT AS \"PRTY_ID\" FROM SUPPLIER_HUB.C_BO_PSTL_ADDR A"
								+ " LEFT JOIN SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR B ON A.ROWID_OBJECT = B.PSTL_ADDR_FK"
								+ " LEFT JOIN SUPPLIER_HUB.C_BO_PRTY C ON C.ROWID_OBJECT = B.PRTY_FK"
								+ " WHERE A.ROWID_OBJECT =" + rowIdobject + "";
				log.info("Inside sql2: " + sql2);
				rsFKDBA = stmtfk.executeQuery(
						"SELECT C.ROWID_OBJECT AS \"PRTY_ID\" FROM SUPPLIER_HUB.C_BO_PSTL_ADDR A"
								+ " LEFT JOIN SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR B ON A.ROWID_OBJECT = B.PSTL_ADDR_FK"
								+ " LEFT JOIN SUPPLIER_HUB.C_BO_PRTY C ON C.ROWID_OBJECT = B.PRTY_FK"
								+ " WHERE A.ROWID_OBJECT =" + rowIdobject + "");
				log.info("PRTY_ID " + rsFKDBA);
				while (rsFKDBA.next()) {
					PRTY_ID = rsFKDBA.getString("PRTY_ID");
					log.info("PRTY_ID " + PRTY_ID);
				}

				// Now you can use PRTY_ID safely
				System.out.println("Party ID: " + PRTY_ID);
				
				log.info("PRTY_ID_SQ12:" + PRTY_ID);
				ResultSet mainT = stmt4.executeQuery("SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_SUPP_MAINTNC_DTLS_XREF WHERE X_PRTY_FK  = '" + PRTY_ID + "' AND HUB_STATE_IND = '0' ORDER BY LAST_UPDATE_DATE DESC FETCH FIRST 1 ROWS ONLY");
				String rowid = null;
				log.info("rowid_ROWID_OBJECT: " + mainT);
				while (mainT.next()) {
					rowid = mainT.getString("ROWID_OBJECT");
					log.info("ROWID_OBJECT_MAINT " + mainT.getString("ROWID_OBJECT"));
				}
				
				ResultSet DateSts = stmt5.executeQuery("SELECT X_INACTIV_DT, X_STS FROM SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF WHERE ROWID_OBJECT = '" + rowIdobject + "' AND HUB_STATE_IND = '0'");
				String EndD = null;
				String Sts = null;
				while (DateSts.next()) {
					log.info("EndD " + DateSts.getString("X_INACTIV_DT"));
					EndD = DateSts.getString("X_INACTIV_DT");
					log.info("X_STS " + DateSts.getString("X_STS"));
					Sts = DateSts.getString("X_STS");
				}
				
				if (PRTY_ID != null && rowid == null) {
					log.info("Found Party ID ");
					PopulateLocationLookup locLookup = new PopulateLocationLookup();
					locLookup.PutCallForLocLookupUpdate(rowIdobject, stmt, stmt2, stmt3, tablename, PRTY_ID);
					//Update Bank details table
					locLookup.PutCallForLookupBnkDtlsUpdate(rowIdobject, stmt, stmt2, stmt3, tablename);			
					log.info("Line 1271");
					Child = stmt.executeQuery("SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XT_LOC_NM WHERE X_LOC_ID = " + rowIdobject + "");
					String XT_ROWID = "";
					while (Child.next()) {
						log.info("XT_ROWID 1275" + Child.getString("ROWID_OBJECT"));
						XT_ROWID = Child.getString("ROWID_OBJECT");
					}
					Child = stmt.executeQuery("SELECT PRTY_FK FROM SUPPLIER_HUB.C_BR_PRTY_PSTL_ADDR WHERE PSTL_ADDR_FK = " + rowIdobject + "");
					String prtyRowIdobject2 = "";
					while (Child.next()) {
						log.info("prtyRowIdobject 1275" + Child.getString("PRTY_FK"));
						prtyRowIdobject2 = Child.getString("PRTY_FK");
					}
					
					log.info("Before if loop 1279");
					log.info("XT_ROWID - " + XT_ROWID + "Status - " + status);
					log.info("Condition1 : " + (status.equals("A")));
					log.info("Condition2 : " + XT_ROWID.isEmpty());
					log.info("End Date - " + baseObjectDataMap.get("X_INACTIV_DT"));
					
					if(baseObjectDataMap.get("X_INACTIV_DT") == null) {
						if((status.equals("A")) && XT_ROWID.isEmpty()) {
							log.info("Status - " + status + "XT_ROWID - " + XT_ROWID);
							PopulateLocationLookup locLookupinsert = new PopulateLocationLookup();
							locLookupinsert.PutCallForLocLookup(rowIdobject, prtyRowIdobject2, stmt, stmt2, stmt3, tablename); //changed for maintenance
							locLookupinsert.PutCallForLocLookupUpdate(rowIdobject, stmt, stmt2, stmt3, tablename, PRTY_ID);
						}
						else if(status.equals("A") && !XT_ROWID.isEmpty()){
							log.info("Inside else loop 1295.");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LOC_NM SET HUB_STATE_IND = 1 WHERE X_LOC_ID = '" + rowIdobject + "'");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LOC_NM_XREF SET HUB_STATE_IND = 1 WHERE X_LOC_ID = '" + rowIdobject + "'");
						}
						else if(status.equals("I") && !XT_ROWID.isEmpty()) {
							log.info("Inside else loop 1299.");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LOC_NM SET HUB_STATE_IND = -1 WHERE X_LOC_ID = '" + rowIdobject + "'");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LOC_NM_XREF SET HUB_STATE_IND = -1 WHERE X_LOC_ID = '" + rowIdobject + "'");
							
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_INACTIV_DT = SYSDATE WHERE ROWID_OBJECT = '" + rowIdobject + "'");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_INACTIV_DT = SYSDATE WHERE ROWID_OBJECT ='" + rowIdobject + "'");
						}
					}
					
					else if(baseObjectDataMap.get("X_INACTIV_DT") != null){
						log.info("Inactive Date is not null.");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_STS = 'I' WHERE X_INACTIV_DT < SYSDATE +1 AND ROWID_OBJECT = '" + rowIdobject +"'");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_STS = 'I' WHERE X_INACTIV_DT < SYSDATE +1 AND ROWID_OBJECT = '" + rowIdobject +"'");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LOC_NM SET HUB_STATE_IND = -1 WHERE trim(X_LOC_ID) IN (SELECT trim(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PSTL_ADDR "
								+ "WHERE X_INACTIV_DT < SYSDATE "
								+ "AND ROWID_OBJECT = '" +rowIdobject+ "')");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LOC_NM_XREF SET HUB_STATE_IND = -1 WHERE trim(X_LOC_ID) IN (SELECT trim(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PSTL_ADDR "
								+ "WHERE X_INACTIV_DT < SYSDATE "
								+ "AND ROWID_OBJECT = '" +rowIdobject+ "')");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LOC_NM SET HUB_STATE_IND = 1 WHERE trim(X_LOC_ID) IN (SELECT trim(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PSTL_ADDR "
								+ "WHERE X_INACTIV_DT > SYSDATE "
								+ "AND ROWID_OBJECT = '" +rowIdobject+ "')");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LOC_NM_XREF SET HUB_STATE_IND = 1 WHERE trim(X_LOC_ID) IN (SELECT trim(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PSTL_ADDR "
								+ "WHERE X_INACTIV_DT > SYSDATE "
								+ "AND ROWID_OBJECT = '" +rowIdobject+ "')");
						
						}
				} else if(rowid != null) {
					log.info("Rowid is not null");
					if(Sts != null) {
						if(Sts.equals("I") && EndD == null) {
							log.info("Status - " + status);
							//stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_INACTIV_DT = SYSDATE WHERE ROWID_OBJECT = '" + rowIdobject + "'");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_INACTIV_DT = SYSDATE WHERE ROWID_OBJECT ='" + rowIdobject + "'");
						}
					}
					else if(EndD != null) {
						log.info("Status - " + status);
						log.info("End Date - " + EndD);
						//stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR SET X_STS = 'I' WHERE X_INACTIV_DT < SYSDATE +1 AND ROWID_OBJECT = '" + rowIdobject +"'");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PSTL_ADDR_XREF SET X_STS = 'I' WHERE X_INACTIV_DT < SYSDATE +1 AND ROWID_OBJECT = '" + rowIdobject +"'");
					}
				}
				stmtfk.close();
				stmt3.close();
				stmt2.close();
				stmt1.close();
				stmt.close();
				con.close();
				
			} catch (Exception e) {
				log.info("Exception in C_XO_SUPP_ACC_BU_DDP DB connection creation " + e);
			}
			finally{
				try { stmtfk.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { stmt3.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { stmt2.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { stmt1.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }
				try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
			}
		}

//---------------------------------------------------C_BO_PRTY_BNK_DTLS--------------------------------------------------------------
			log.info("To add DBA name " + tablename);
				// Generating objectId for C_BO_PRTY_BNK_DTLS
				if (tablename != null && tablename.equals("C_BO_PRTY_BNK_DTLS")) {
					Set<String> setBOKey = baseObjectDataMap.keySet();
					if (setBOKey != null) {
						Iterator<String> itrBOKey = setBOKey.iterator();
						if (itrBOKey != null) {
							while (itrBOKey.hasNext()) {
								String stBOKey = (String) itrBOKey.next();
								Object objBO = (Object) baseObjectDataMap.get(stBOKey);
								if (objBO instanceof String) {
									String stValue = (String) objBO;
									if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
										rowIdobject = stValue;
										rowIdobject = rowIdobject.trim();
										log.info("C_BO_PRTY_BNK_DTLS rowIdObject" + rowIdobject);
									}
									if (stBOKey != null && stBOKey.equals("PRTY_FK")) {
										prtyRowIdobject = stValue;
										prtyRowIdobject = prtyRowIdobject.trim();
										log.info("C_BO_PRTY_BNK_DTLS Party ID rowIdObject" + prtyRowIdobject);
									}
									if (stBOKey != null && stBOKey.equals("ACCNT_STS")) {
										status = stValue;
										status = status.trim();
										log.info("C_BO_PRTY_BNK_DTLS ACCNT_STS" + status);
									}
								}
							}
						}
					}
					log.info("Begin DB C_BO_PRTY_BNK_DTLS Connection creation ");
					Context ctx = null;
					Connection con = null;
					Statement stmt = null;
					ResultSet XTtable = null;
					ResultSet HubState = null;
					try {
		
						ctx = new InitialContext();
						DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
						con = ds.getConnection();
						stmt = con.createStatement();
						
						XTtable = stmt.executeQuery("SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID WHERE X_PMNT_BNK_DTLS_DDP_ROWID = " + rowIdobject + "");
						String XT_ROWID = "";
						while (XTtable.next()) {
							log.info("XT_ROWID- " + XTtable.getString("ROWID_OBJECT"));
							XT_ROWID = XTtable.getString("ROWID_OBJECT");
						}
						
						/*HubState = stmt.executeQuery("SELECT HUB_STATE_IND FROM SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS WHERE ROWID_OBJECT = " + rowIdobject + " AND HUB_STATE_IND = '1'");
						String hubstateind = "";
						while (HubState.next()) {
							log.info("HUB_STATE_IND- " + HubState.getString("HUB_STATE_IND"));
							hubstateind = HubState.getString("HUB_STATE_IND");
						}*/
						
						log.info("Status - " + status);
						log.info("End Date - " + baseObjectDataMap.get("EFF_END_DT"));
						//log.info("HUB_STATE_IND - " + hubstateind);
						
						if(baseObjectDataMap.get("EFF_END_DT") == null) {
							if((status.equals("ACTIVE")) && XT_ROWID.isEmpty()) {
								BankDetailsIdDDP bankdDetailsID = new BankDetailsIdDDP();
								bankdDetailsID.BankDetailsDDPLookup(rowIdobject, stmt, prtyRowIdobject, tablename);  //changed for Maintenance
							}
							else if(status.equals("ACTIVE") && !XT_ROWID.isEmpty()){
								log.info("Inside else loop 1474.");
								stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID SET HUB_STATE_IND = 1 WHERE X_PMNT_BNK_DTLS_DDP_ROWID = '" + rowIdobject + "'");
								stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID_XREF SET HUB_STATE_IND = 1 WHERE X_PMNT_BNK_DTLS_DDP_ROWID = '" + rowIdobject + "'");
							}
							else if((status.equals("INACTIVE")) && !XT_ROWID.isEmpty()) {
								stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID SET HUB_STATE_IND = -1 WHERE X_PMNT_BNK_DTLS_DDP_ROWID = '" + rowIdobject + "'");
								stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID_XREF SET HUB_STATE_IND = -1 WHERE X_PMNT_BNK_DTLS_DDP_ROWID = '" + rowIdobject + "'");
								
								stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS SET EFF_END_DT = SYSDATE WHERE ROWID_OBJECT = '" + rowIdobject + "'");
								stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS_XREF SET EFF_END_DT = SYSDATE WHERE ROWID_OBJECT ='" + rowIdobject + "'");
							}
						}
						else if(baseObjectDataMap.get("EFF_END_DT") != null){
							log.info("Effective End Date is not null.");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS SET ACCNT_STS = 'INACTIVE' WHERE EFF_END_DT < SYSDATE +1 AND ROWID_OBJECT = '" + rowIdobject +"'");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS_XREF SET ACCNT_STS = 'INACTIVE' WHERE EFF_END_DT < SYSDATE +1 AND ROWID_OBJECT = '" + rowIdobject +"'");
							
							log.info("Update 1 - 1491");
							String sql1 = "UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID SET HUB_STATE_IND = -1"
									+ "WHERE X_PMNT_BNK_DTLS_DDP_ROWID ="
									+ "(SELECT RTRIM(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS WHERE ROWID_OBJECT ='" + rowIdobject + "' AND "
									+ "EFF_END_DT < SYSDATE )";
							log.info("SQl 1 - " +sql1);
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID SET HUB_STATE_IND = -1"
									+ "WHERE X_PMNT_BNK_DTLS_DDP_ROWID ="
									+ "(SELECT RTRIM(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS WHERE ROWID_OBJECT ='" + rowIdobject + "' AND "
									+ "EFF_END_DT < SYSDATE )");
							
							log.info("Update XREF 1 - 1498");
							String sqlXref1 = "UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID_XREF SET HUB_STATE_IND = -1"
									+ "WHERE X_PMNT_BNK_DTLS_DDP_ROWID ="
									+ "(SELECT RTRIM(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS WHERE ROWID_OBJECT ='" + rowIdobject + "' AND "
									+ "EFF_END_DT < SYSDATE )";
							log.info("SQl XREF 1 - " +sqlXref1);
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID_XREF SET HUB_STATE_IND = -1"
									+ "WHERE X_PMNT_BNK_DTLS_DDP_ROWID ="
									+ "(SELECT RTRIM(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS WHERE ROWID_OBJECT ='" + rowIdobject + "' AND "
									+ "EFF_END_DT < SYSDATE )");
							
							log.info("Update 2 - 1505");
							String sql2 = "UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID SET HUB_STATE_IND = 1"
									+ "WHERE X_PMNT_BNK_DTLS_DDP_ROWID ="
									+ "(SELECT RTRIM(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS WHERE ROWID_OBJECT ='" + rowIdobject + "' AND "
									+ "EFF_END_DT > SYSDATE )";
							log.info("SQl XREF 1 - " +sql2);
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID SET HUB_STATE_IND = 1"
									+ "WHERE X_PMNT_BNK_DTLS_DDP_ROWID ="
									+ "(SELECT RTRIM(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS WHERE ROWID_OBJECT ='" + rowIdobject + "' AND "
									+ "EFF_END_DT > SYSDATE )");
							
							log.info("Update XREF 2 - 1512");
							String sqlXref2 = "UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID_XREF SET HUB_STATE_IND = 1"
									+ "WHERE X_PMNT_BNK_DTLS_DDP_ROWID ="
									+ "(SELECT RTRIM(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS WHERE ROWID_OBJECT ='" + rowIdobject + "' AND "
									+ "EFF_END_DT > SYSDATE )";
							log.info("SQl XREF 1 - " +sqlXref2);
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_BANK_ACC_ID_XREF SET HUB_STATE_IND = 1"
									+ "WHERE X_PMNT_BNK_DTLS_DDP_ROWID ="
									+ "(SELECT RTRIM(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS WHERE ROWID_OBJECT ='" + rowIdobject + "' AND "
									+ "EFF_END_DT > SYSDATE )");
							
							}
						/*else if (XT_ROWID.isEmpty()) {
							if(status.equals("INACTIVE") && baseObjectDataMap.get("EFF_END_DT") == null) {
								stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS SET EFF_END_DT = SYSDATE WHERE ROWID_OBJECT = '" + rowIdobject + "'");
								stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS_XREF SET EFF_END_DT = SYSDATE WHERE ROWID_OBJECT ='" + rowIdobject + "'");
							}
							else if (baseObjectDataMap.get("EFF_END_DT") != null) {
								stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS SET ACCNT_STS = 'INACTIVE' WHERE EFF_END_DT < SYSDATE +1 AND ROWID_OBJECT = '" + rowIdobject +"'");
								stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_BNK_DTLS_XREF SET ACCNT_STS = 'INACTIVE' WHERE EFF_END_DT < SYSDATE +1 AND ROWID_OBJECT = '" + rowIdobject +"'");
							}	
						}*/
						// Soft delete for DNB Check
						stmt.close();
						con.close();
		
					} catch (Exception e) {
						log.info("Exception in C_BO_PRTY_BNK_DTLS DB connection creation " + e.getStackTrace()[0].getLineNumber());
					}
					finally{
						try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
						try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
					}
				}

//----------------------------------------------------------C_BR_PRTY_PSTL_ADDR---------------------------------------------------------------
			log.info("To add Address name " + tablename);
			// Generating objectId for C_XR_SUPP_ACC_BU_REL
			String PSTL_ADDR_FK = null;
			if (tablename != null && tablename.equals("C_BR_PRTY_PSTL_ADDR")) {
				log.info("POST_LOADNew baseObjectDataMap " + baseObjectDataMap);
				Set<String> setBOKey = baseObjectDataMap.keySet();
				if (setBOKey != null) {
					Iterator<String> itrBOKey = setBOKey.iterator();
					if (itrBOKey != null) {
						while (itrBOKey.hasNext()) {
							String stBOKey = (String) itrBOKey.next();
							Object objBO = (Object) baseObjectDataMap.get(stBOKey);
							if (objBO instanceof String) {
								String stValue = (String) objBO;
								if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
									rowIdobject = stValue;
									rowIdobject = rowIdobject.trim();
									log.info("C_BR_PRTY_PSTL_ADDR rowIdObject" + rowIdobject);
								}
								if (stBOKey != null && stBOKey.equals("PRTY_FK")) {
									prtyRowIdobject = stValue;
									prtyRowIdobject = prtyRowIdobject.trim();
									log.info("C_BR_PRTY_PSTL_ADDR rowIdObject" + prtyRowIdobject);
								}
								if (stBOKey != null && stBOKey.equals("PSTL_ADDR_FK")) {
									PSTL_ADDR_FK = stValue;
									PSTL_ADDR_FK = PSTL_ADDR_FK.trim();
									log.info("C_BR_PRTY_PSTL_ADDR PSTL_ADDR_FK" + PSTL_ADDR_FK);
								}
							}
						}
					}
				}
				log.info("Begin DB C_BR_PRTY_PSTL_ADDR Connection creation ");
				Context ctx = null;
				Connection con = null;
				Statement stmt = null;
				Statement stmt2 = null;
				Statement stmt3 = null;
				Statement stmtfk = null;
				try {
					ctx = new InitialContext();
					DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
					con = ds.getConnection();
					stmt = con.createStatement();
					stmt2 = con.createStatement();
					stmt3 = con.createStatement();
					stmtfk = con.createStatement();
		
					
					
					//To get Supplier name with help of Party RowIDObject
					ResultSet rsFKDBA = null;
					String XFULL_NM = "";
					log.info("Before C_BO_PSTL_ADDR Select ");
					String sql2 = "SELECT FULL_NM AS \"XFULL_NM\" FROM SUPPLIER_HUB.C_BO_PRTY" + " WHERE ROWID_OBJECT ="
							+ prtyRowIdobject + "";
					log.info("Inside sql2" + sql2);
					rsFKDBA = stmtfk.executeQuery("SELECT FULL_NM AS \"XFULL_NM\" FROM SUPPLIER_HUB.C_BO_PRTY"
							+ " WHERE ROWID_OBJECT =" + prtyRowIdobject + "");
					while (rsFKDBA.next()) {
						log.info("XFULL_NM " + rsFKDBA.getString("XFULL_NM"));
						XFULL_NM = rsFKDBA.getString("XFULL_NM");
					}
		
					ResultSet rsFKDBA1 = null;
					String XSUPP_NM = null;
					log.info("Before C_BO_PSTL_ADDR Select ");
					String sql21 = "SELECT X_SUPP_NM AS XSUPP_NM FROM SUPPLIER_HUB.C_XT_PRTY_FK" + " WHERE X_PRTY_FK ="
							+ prtyRowIdobject + "";
					log.info("Inside sql2" + sql21);
					rsFKDBA1 = stmtfk.executeQuery("SELECT X_SUPP_NM AS XSUPP_NM FROM SUPPLIER_HUB.C_XT_PRTY_FK"
							+ " WHERE X_PRTY_FK =" + prtyRowIdobject + "");
					while (rsFKDBA1.next()) {
						log.info("XFULL_NM " + rsFKDBA1.getString("XSUPP_NM"));
						XSUPP_NM = rsFKDBA1.getString("XSUPP_NM");
					}
					if (prtyRowIdobject != null && XSUPP_NM != null) {
						log.info("Parent entry already exist");
					} else {
						GenerateDBAaltID dba = new GenerateDBAaltID();
						log.info("InsideNewCall PutCallForParentLookup ");
						dba.PutCallForParentLookup(prtyRowIdobject, stmt, stmt2, stmt3, tablename, XFULL_NM);
					}
		
					if (prtyRowIdobject != null) {
						log.info("Got Party ID ");
						PopulateLocationLookup locLookup = new PopulateLocationLookup();
						locLookup.PutCallForLocLookup(PSTL_ADDR_FK, prtyRowIdobject, stmt, stmt2, stmt3, tablename);	
					}
					else {
						log.info("Party ID is null line 2416");
					}
					rsFKDBA1.close();
					rsFKDBA.close();
					stmtfk.close();
					stmt3.close();
					stmt2.close();
					stmt.close();
					con.close();
				} catch (Exception e) {
					log.info("Exception in C_BR_PRTY_PSTL_ADDR DB connection creation " + e);
				}
				finally{
					try { stmtfk.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
					try { stmt3.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
					try { stmt2.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
					try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
					try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
				}
			}

//------------------------------------------------------------C_XR_BNK_ADDR_REL------------------------------------------------------------------		
		log.info("To add DBA name " + tablename);
		// Generating LocId for C_XO_SUPP_ACC_DDP
		if (tablename != null && tablename.equals("C_XR_BNK_ADDR_REL")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_XO_SUPP_ACC_DDP_POST_LOAD rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
								prtyRowIdobject = stValue;
								prtyRowIdobject = prtyRowIdobject.trim();
								log.info("C_XR_BNK_ADDR_REL Party ID rowIdObject" + prtyRowIdobject);
							}
						}
					}
				}
			}
			log.info("Begin DB C_XR_BNK_ADDR_REL Connection creation ");
		//Ankit: Date:4/11/2025
			
			DatabaseConnection dbHandler = new DatabaseConnection();
            Statement stmt = null;

    try {
        stmt = dbHandler.createConnection();
        GenerateLocId prchsfrmId = new GenerateLocId();
		prchsfrmId.GenerateUniqueLocID(rowIdobject, stmt,prtyRowIdobject);
		// Soft delete for DNB Check

    } catch (Exception e) {
    	log.info("Exception in C_XR_BNK_ADDR_REL DB connection creation " + e);
    	} finally {
        dbHandler.closeResources();
    }		
//End Ankit: Date:4/11/2025
		
	}

//---------------------------------------------------------C_BO_PRTY_TAX_DTLS-------------------------------------------------------------------
		log.info("To add DBA name " + tablename);
		// Generating objectId for C_XR_SUPP_ACC_BU_REL
		String prtyrowIdobject = null;
		if (tablename != null && tablename.equals("C_BO_PRTY_TAX_DTLS")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_BO_PRTY_TAX_DTLS RowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("PRTY_FK")) {
								prtyrowIdobject = stValue;
								prtyrowIdobject = prtyrowIdobject.trim();
								log.info("C_BO_PRTY_TAX_DTLS PrtyRowIdobject" + prtyrowIdobject);
							}
						}
					}
				}
			}
			log.info("Begin DB C_BO_PRTY_TAX_DTLS Connection creation ");
			Context ctx = null;
			Connection con = null;
			Statement stmt = null;
			Statement stmt1 = null;
			Statement stmt2 = null;
			Statement stmtfk = null;
			try {
				ctx = new InitialContext();
				DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
				con = ds.getConnection();
				stmt = con.createStatement();
				stmt1 = con.createStatement();
				stmt2 = con.createStatement();
				stmtfk = con.createStatement();
				ResultSet rsFKDBA = null;
				ResultSet XTtable = null;
				String taxRegNum = "";
				String XT_ROWID = "";
				
				rsFKDBA = stmtfk.executeQuery("SELECT TAX_NUM FROM SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS WHERE HUB_STATE_IND = '1' AND ROWID_OBJECT =" + rowIdobject + "");
				
				while (rsFKDBA.next()) {
					taxRegNum = rsFKDBA.getString("TAX_NUM");
					log.info("Tax Reg Num - " + taxRegNum);
				}
				
				XTtable = stmt.executeQuery("SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM WHERE X_PMNT_TAX_RGSTR_ROWID = " + rowIdobject + "");
				
				while (XTtable.next()) {
					log.info("XT_ROWID- " + XTtable.getString("ROWID_OBJECT"));
					XT_ROWID = XTtable.getString("ROWID_OBJECT");
				}
				
				log.info("End Date - " + baseObjectDataMap.get("EXPRY_DT"));
				
				if(!taxRegNum.isEmpty()) {
					if(baseObjectDataMap.get("EXPRY_DT") == null) {
						if(XT_ROWID.isEmpty()) {
							TaxRegNum taxReg = new TaxRegNum();
							log.info("Insert into XT");
							taxReg.TaxRegNumLookup(rowIdobject, stmt, prtyrowIdobject, taxRegNum, tablename);
						}
						else if(!XT_ROWID.isEmpty()){
							log.info("Inside else loop 1474.");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM SET HUB_STATE_IND = 1 WHERE X_PMNT_TAX_RGSTR_ROWID = '" + rowIdobject + "'");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM_XREF SET HUB_STATE_IND = 1 WHERE X_PMNT_TAX_RGSTR_ROWID = '" + rowIdobject + "'");
							
							TaxRegNum taxReg = new TaxRegNum();
							log.info("Insert into XT");
							taxReg.TaxRegNumUpdate(rowIdobject, stmt, prtyrowIdobject, taxRegNum, tablename);
						}
					}
					else if(baseObjectDataMap.get("EXPRY_DT") != null){
						log.info("Expiry Date is not null.");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM SET HUB_STATE_IND = -1"
								+ "WHERE X_PMNT_TAX_RGSTR_ROWID ="
								+ "(SELECT RTRIM(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS WHERE ROWID_OBJECT ='" + rowIdobject + "' AND "
								+ "EXPRY_DT < SYSDATE )");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM_XREF SET HUB_STATE_IND = -1"
								+ "WHERE X_PMNT_TAX_RGSTR_ROWID ="
								+ "(SELECT RTRIM(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS WHERE ROWID_OBJECT ='" + rowIdobject + "' AND "
								+ "EXPRY_DT < SYSDATE )");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM SET HUB_STATE_IND = 1"
								+ "WHERE X_PMNT_TAX_RGSTR_ROWID ="
								+ "(SELECT RTRIM(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS WHERE ROWID_OBJECT ='" + rowIdobject + "' AND "
								+ "EXPRY_DT > SYSDATE )");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_PMNT_TAX_RGSTR_NUM_XREF SET HUB_STATE_IND = 1"
								+ "WHERE X_PMNT_TAX_RGSTR_ROWID ="
								+ "(SELECT RTRIM(ROWID_OBJECT) FROM SUPPLIER_HUB.C_BO_PRTY_TAX_DTLS WHERE ROWID_OBJECT ='" + rowIdobject + "' AND "
								+ "EXPRY_DT > SYSDATE )");
						
						}
				}
				else {
					log.info("Do nothing here");
				}
				
				// Soft delete for DNB Check
				stmtfk.close();
				stmt2.close();
				stmt1.close();
				stmt.close();
				con.close();

			} catch (Exception e) {
				log.info("Exception in C_BO_PRTY_TAX_DTLS DB connection creation " + e);
			}
			finally{
				try { stmtfk.close();} catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { stmt2.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { stmt1.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
			}
		}

//--------------------------------------------------------C_XO_PRTY_CNTCT---------------------------------------------------------------------
		log.info("To add C_XO_PRTY_CNTCT " + tablename);
		String emailId = null;
		String lastNm = null;
		String stsCd = null;
		// Generating objectId for C_BO_PRTY_NM
		if (tablename != null && tablename.equals("C_XO_PRTY_CNTCT")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_XO_PRTY_CNTCT rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
								prtyRowIdobject = stValue;
								prtyRowIdobject = prtyRowIdobject.trim();
								log.info("C_XO_PRTY_CNTCT Party_FK" + prtyRowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("X_ETRNC_ADDR")) {
								emailId = stValue;
								emailId = emailId.trim();
								log.info("C_XO_PRTY_CNTCT EmailId" + emailId);
							}
							if (stBOKey != null && stBOKey.equals("X_LST_NM")) {
								lastNm = stValue;
								lastNm = lastNm.trim();
								log.info("C_XO_PRTY_CNTCT LastNm" + lastNm);
							}
							if (stBOKey != null && stBOKey.equals("X_STS_CD")) {
								stsCd = stValue;
								stsCd = stsCd.trim();
								log.info("C_XO_PRTY_CNTCT Status Code" + stsCd);
							}
						}
					}
				}
			}
			log.info("Begin DB C_XO_PRTY_CNTCT Connection creation ");
			Context ctx = null;
			Connection con = null;
			Statement stmt = null;
			Statement stmt2 = null;
			Statement stmt3 = null;
			Statement stmt4 = null;
			Statement stmt5 = null;
			Statement stmt6 = null;
			Statement stmt7 = null;
			Statement stmt8 = null;
			Statement stmtfk = null;
			try {

				ctx = new InitialContext();
				DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
				con = ds.getConnection();
				stmt = con.createStatement();
				stmt2 = con.createStatement();
				stmt3 = con.createStatement();
				stmt4 = con.createStatement();
				stmt5 = con.createStatement();
				stmt6 = con.createStatement();
				stmt7 = con.createStatement();
				stmt8 = con.createStatement();
				stmtfk = con.createStatement();
				ResultSet childCntct = null;
				ResultSet childCntct2 = null;
				ResultSet childCntct3 = null;
				ResultSet childCntct4 = null;
				ResultSet HubState = null;
				String childRowIdObect = null;
				String cnctlstNm = null;
				String prtyPmntCnctRowid = null;
				String prtyPmntCnctRowidAT = null;
				String hubstateind = null;
				
				
				HubState = stmt.executeQuery("SELECT HUB_STATE_IND FROM SUPPLIER_HUB.C_XO_PRTY_CNTCT WHERE ROWID_OBJECT = '" + rowIdobject + "' AND HUB_STATE_IND = '1'");
				while (HubState.next()) {
					log.info("HUB_STATE_IND " + HubState.getString("HUB_STATE_IND"));
					hubstateind = HubState.getString("HUB_STATE_IND");
				}
				
				log.info("Hub State value - " + hubstateind);
				
				if(hubstateind != null) {
				AssociateContact emailLastnm = new AssociateContact();
				log.info("Insert into XT");
				emailLastnm.AssociateContactLookup(rowIdobject, prtyRowIdobject, stmt, emailId, lastNm, tablename); //changed for maintenance
				}
				
				childCntct = stmt.executeQuery("SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT WHERE X_CNCT_ROW_ID = '" + rowIdobject + "'");
				while (childCntct.next()) {
					log.info("ROWID_OBJECT " + childCntct.getString("ROWID_OBJECT"));
					childRowIdObect = childCntct.getString("ROWID_OBJECT");
				}
				
				log.info("Parent Rowid_Object - " + rowIdobject);
				log.info("Child Rowid_Object - " + childRowIdObect);
				log.info("Parent Status - " + stsCd);
				//log.info("Parent Inactive Date - " + baseObjectDataMap.get("X_INACT_DT"));
				
				ResultSet Xref = stmt7.executeQuery("SELECT X_INACT_DT, X_STS_CD FROM SUPPLIER_HUB.C_XO_PRTY_CNTCT_XREF WHERE ROWID_OBJECT = " + rowIdobject + " AND HUB_STATE_IND = '0'");
				String EndD = null;
				String StsXref = null;
				while (Xref.next()) {
					log.info("X_INACT_DT " + Xref.getString("X_INACT_DT"));
					EndD = Xref.getString("X_INACT_DT");
					log.info("X_STS_CD " + Xref.getString("X_STS_CD"));
					StsXref = Xref.getString("X_STS_CD");
				}
				
				ResultSet mainT = stmt8.executeQuery("SELECT ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_SUPP_MAINTNC_DTLS_XREF WHERE X_PRTY_FK  = '" + prtyRowIdobject + "' AND HUB_STATE_IND = '0' ORDER BY LAST_UPDATE_DATE DESC FETCH FIRST 1 ROWS ONLY");
				String rowid = null;
				while (mainT.next()) {
					log.info("ROWID_OBJECT_MAINT " + mainT.getString("ROWID_OBJECT"));
					rowid = mainT.getString("ROWID_OBJECT");
				}
				
				if(rowid == null) {
				if(baseObjectDataMap.get("X_INACT_DT") == null){
					if(stsCd.equals("A") && childRowIdObect.isEmpty() && hubstateind != null) {
						log.info("Approved and Child is Empty");
						AssociateContact emailLastnm = new AssociateContact();
						emailLastnm.AssociateContactLookup(rowIdobject, prtyRowIdobject, stmt, emailId, lastNm, tablename);
					}
					else if(stsCd.equals("I") && !childRowIdObect.isEmpty()) {
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT SET HUB_STATE_IND = -1 WHERE X_CNCT_ROW_ID = '" + rowIdobject + "'");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT_XREF SET HUB_STATE_IND = -1 WHERE X_CNCT_ROW_ID = '" + rowIdobject + "'");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT SET X_INACT_DT = SYSDATE WHERE ROWID_OBJECT = '" + rowIdobject + "'");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT_XREF SET X_INACT_DT = SYSDATE WHERE ROWID_OBJECT = '" + rowIdobject + "'");
					}
					else if(stsCd.equals("A") && !childRowIdObect.isEmpty()) {
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT SET HUB_STATE_IND = 1 WHERE X_CNCT_ROW_ID = '" + rowIdobject + "'");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT_XREF SET HUB_STATE_IND = 1 WHERE X_CNCT_ROW_ID = '" + rowIdobject + "'");
					}
					else {
						log.info("Status is Null and End Date is Null.");
					}
				}
				
				else if(baseObjectDataMap.get("X_INACT_DT") != null){
					log.info("Parent Inactive Date - " + baseObjectDataMap.get("X_INACT_DT"));
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT SET X_STS_CD = 'I' WHERE X_INACT_DT < SYSDATE +1 AND ROWID_OBJECT = '" + rowIdobject +"'");
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT_XREF SET X_STS_CD = 'I' WHERE X_INACT_DT < SYSDATE +1 AND ROWID_OBJECT = '" + rowIdobject +"'");
					
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT SET HUB_STATE_IND = -1 WHERE trim(X_CNCT_ROW_ID) IN (SELECT trim(ROWID_OBJECT) FROM SUPPLIER_HUB.C_XO_PRTY_CNTCT "
							+ "WHERE X_INACT_DT < SYSDATE "
							+ "AND ROWID_OBJECT = '" +rowIdobject+ "')");
					
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT SET HUB_STATE_IND = 1 WHERE trim(X_CNCT_ROW_ID) IN (SELECT trim(ROWID_OBJECT) FROM SUPPLIER_HUB.C_XO_PRTY_CNTCT "
							+ "WHERE X_INACT_DT > SYSDATE "
							+ "AND ROWID_OBJECT = '" +rowIdobject+ "')");
					}
				
				else if (!childRowIdObect.isEmpty() && stsCd == null) {
					log.info("Updating XT table ");
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT SET X_EMAIL_ADDR = '" + emailId + "' WHERE trim(X_CNCT_ROW_ID) IN (SELECT trim(ROWID_OBJECT) FROM SUPPLIER_HUB.C_XO_PRTY_CNTCT "
							+ "WHERE ROWID_OBJECT = '" +rowIdobject+ "')");
					
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT_XREF"
							+" SET X_EMAIL_ADDR = '" + emailId + "' WHERE ROWID_XREF = '" + rowIdobject + "'");
					
					}
				}
				
				if(rowid != null) {
					if(StsXref != null) {
						if(StsXref.equals("I") && EndD == null) {
							//stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT SET X_INACT_DT = SYSDATE WHERE ROWID_OBJECT = '" + rowIdobject + "'");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT_XREF SET X_INACT_DT = SYSDATE WHERE ROWID_OBJECT = '" + rowIdobject + "'");
						}
					}
					if(EndD != null) {
						//stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT SET X_STS_CD = 'I' WHERE X_INACT_DT < SYSDATE +1 AND ROWID_OBJECT = '" + rowIdobject +"'");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_CNTCT_XREF SET X_STS_CD = 'I' WHERE X_INACT_DT < SYSDATE +1 AND ROWID_OBJECT = '" + rowIdobject +"'");
					}
				}
				
				log.info("2305 Update Child associated records C_XO_PRTY_PMNT_CNCT, C_XO_PMNT_CNCT_ACCU_TECH");

				childCntct2 = stmt4.executeQuery("SELECT X_LST_NM FROM SUPPLIER_HUB.C_XO_PRTY_CNTCT cxpc WHERE HUB_STATE_IND = '1' AND ROWID_OBJECT ='" + rowIdobject + "'");
				while (childCntct2.next()) {
					log.info("X_LST_NM " + childCntct2.getString("X_LST_NM"));
					cnctlstNm = childCntct2.getString("X_LST_NM");
				}
				if (cnctlstNm!=null)
				{
					childCntct3 = stmt5.executeQuery("SELECT ROWID_OBJECT AS PRTY_PMNT_CNCT_ROWID FROM SUPPLIER_HUB.C_XO_PRTY_PMNT_CNCT cxppc WHERE X_EMAIL_ADDR = '" + rowIdobject + "'");
					while (childCntct3.next()) {
						log.info("PRTY_PMNT_CNCT_ROWID " + childCntct3.getString("PRTY_PMNT_CNCT_ROWID"));
						prtyPmntCnctRowid = childCntct3.getString("PRTY_PMNT_CNCT_ROWID");
						
						if(prtyPmntCnctRowid!=null)
						{
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_CNCT SET X_CNCT_LAST_NM = '" + cnctlstNm
									+ "' WHERE ROWID_OBJECT = '" +prtyPmntCnctRowid+ "'");
							
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PRTY_PMNT_CNCT_XREF SET X_CNCT_LAST_NM = '" + cnctlstNm
									+ "' WHERE ROWID_OBJECT = '" +prtyPmntCnctRowid+ "'");

						}
					}
					
					
					childCntct4 = stmt6.executeQuery("SELECT ROWID_OBJECT AS PMNT_CNCT_AT_ROWID FROM SUPPLIER_HUB.C_XO_PMNT_CNCT_ACCU_TECH cxpcat WHERE X_EMAIL_ADDR  =  '" + rowIdobject + "'");
					while (childCntct4.next()) {
						log.info("PMNT_CNCT_AT_ROWID " + childCntct4.getString("PMNT_CNCT_AT_ROWID"));
						prtyPmntCnctRowidAT = childCntct4.getString("PMNT_CNCT_AT_ROWID");
						
						if(prtyPmntCnctRowidAT!=null)
						{
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_CNCT_ACCU_TECH SET X_CNTCT_LAST_NM = '" + cnctlstNm
									+ "' WHERE ROWID_OBJECT = '" +prtyPmntCnctRowidAT+ "'");
							
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_CNCT_ACCU_TECH_XREF SET X_CNTCT_LAST_NM = '" + cnctlstNm
									+ "' WHERE ROWID_OBJECT = '" +prtyPmntCnctRowidAT+ "'");
							
						}
					}

				}
				
				stmtfk.close();
				stmt8.close(); 
	            stmt7.close();
				stmt6.close(); 
	            stmt5.close();
	            stmt4.close(); 
	            stmt3.close();
	            stmt2.close(); 
	            stmt.close();
	            con.close(); 
				
			} catch (Exception e) {
				log.info("Exception in C_XO_PRTY_CNTCT DB connection creation " + e);
			}
			finally{
				try { stmtfk.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmtfk " + e); }
				try { stmt8.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmtfk " + e); }
				try { stmt7.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmtfk " + e); }
				try { stmt6.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmtfk " + e); }
				try { stmt5.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmtfk " + e); }
				try { stmt4.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt3 " + e); }
				try { stmt3.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt3 " + e); }
				try { stmt2.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt2 " + e); }
				try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
			}
			
		}

//---------------------------------------------------------C_XO_PRTY_PMNT_CNCT-------------------------------------------------------------------
		log.info("To add DBA name " + tablename);
		// Generating LocId for C_XO_SUPP_ACC_DDP
		String emailAddr = null;
		if (tablename != null && tablename.equals("C_XO_PRTY_PMNT_CNCT")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_XO_PRTY_PMNT_CNCT rowIdObject" + rowIdobject);
							}
							if (stBOKey != null && stBOKey.equals("X_EMAIL_ADDR")) {
								emailAddr = stValue;
								emailAddr = emailAddr.trim();
								log.info("C_XO_PRTY_PMNT_CNCT emailAddr" + emailAddr);
							}
						}
					}
				}
			}
			log.info("Begin DB C_XO_PRTY_PMNT_CNCT Connection creation ");
			Context ctx = null;
			Connection con = null;
			Statement stmt = null;
			try {
		
				ctx = new InitialContext();
				DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
				con = ds.getConnection();
				stmt = con.createStatement();
				

				ResultSet childOne = null;
				childOne = stmt.executeQuery("SELECT X_PRTY_FK , X_EMAIL_ADDR  FROM SUPPLIER_HUB.C_XT_LKP_PMNT_CNCT"
						+ " WHERE X_CNCT_ROW_ID =" + emailAddr + "");
				String emailId1 = "";
				String prtyRowIdobject1 = "";
				while (childOne.next()) {
					log.info("X_PRTY_FK " + childOne.getString("X_PRTY_FK"));
					prtyRowIdobject1 = childOne.getString("X_PRTY_FK");
					log.info("X_EMAIL_ADDR " + childOne.getString("X_EMAIL_ADDR"));
					emailId1 = childOne.getString("X_EMAIL_ADDR");
				}
				if(prtyRowIdobject1 != null ) {
				AssociateContact emailLastnm = new AssociateContact();
				log.info("InsideNewCall AssociateContactPmntChild");
				emailLastnm.AssociateContactPmntChild(rowIdobject, prtyRowIdobject1, stmt, emailId1,emailAddr);
				}
				
				stmt.close();
	            con.close(); 
		
			} catch (Exception e) {
				log.info("Exception in C_XO_PRTY_PMNT_CNCT DB connection creation " + e);
			}
			finally{
				try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
			}
		}
		
//---------------------------------------------------------C_XO_SUPP_ACC_BU_DDP-------------------------------------------------------------------
		log.info("To add DBA name " + tablename);
		// Generating objectId for C_XR_SUPP_ACC_BU_REL
		if (tablename != null && tablename.equals("C_XO_SUPP_ACC_BU_DDP")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_XO_SUPP_ACC_BU_DDP RowIdObject" + rowIdobject);
							}
						}
					}
				}
			}
			log.info("Begin DB C_XO_SUPP_ACC_BU_DDP Connection creation ");
			Context ctx = null;
			Connection con = null;
			Statement stmt = null;
			Statement stmt1 = null;
			Statement stmt2 = null;
			Statement stmtfk = null;
			try {

				ctx = new InitialContext();
				DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
				con = ds.getConnection();
				stmt = con.createStatement();
				stmt1 = con.createStatement();
				stmt2 = con.createStatement();
				stmtfk = con.createStatement();
				ResultSet rsFKDBA = null;
				ResultSet buDDP = null;
				//String accDDPRowIdobject = "";
				log.info("Before C_BO_PSTL_ADDR Select ");
				try{
					int ctr=0;
		            Thread waitThread = new Thread();
						while(ctr<=3)
						{
							log.info("We are at line 3168");
			                 Thread.sleep(2000);
			                 String timeStamp1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new java.util.Date());
			                 log.info("timeStamp1 :   "+timeStamp1);
			                 ctr++;
	
						}
					}catch(Exception e) {
						log.info("Exception in While - " + e.getStackTrace().toString());
					}
				String sql2 = "SELECT A.X_PRTY_FK AS \"PRTY_FK\",A.ROWID_OBJECT AS \"ROWID_OBJECT\" FROM SUPPLIER_HUB.C_XO_SUPP_ACC_DDP A"
						+ " LEFT JOIN SUPPLIER_HUB.C_XR_SUPP_ACC_BU_REL B ON A.ROWID_OBJECT = B.X_SUPP_ACC_DDP_FK"
						+ " WHERE B.ROWID_OBJECT ='" + rowIdobject + "'";
				log.info("Inside sql2 line 3164" + sql2);
				rsFKDBA = stmtfk.executeQuery(
						"SELECT A.X_PRTY_FK AS \"PRTY_FK\",A.ROWID_OBJECT AS \"ROWID_OBJECT\" FROM SUPPLIER_HUB.C_XO_SUPP_ACC_DDP A"
								+ " LEFT JOIN SUPPLIER_HUB.C_XR_SUPP_ACC_BU_REL B ON A.ROWID_OBJECT = B.X_SUPP_ACC_DDP_FK"
								+ " WHERE B.ROWID_OBJECT ='" + rowIdobject + "'");
				
				String prtyRowIdobject1 = "";
				
				log.info("Before while loop - 3172");
				while (rsFKDBA.next()) {
					log.info("Inside while loop - 3174");
					log.info("PRTY_FK " + rsFKDBA.getString("PRTY_FK"));
					log.info("ROWID_OBJECT " + rsFKDBA.getString("ROWID_OBJECT"));
					prtyRowIdobject1 = rsFKDBA.getString("PRTY_FK");
				}
				log.info("After while loop - 3179");
				
				log.info("Before C_BO_PSTL_ADDR Select ");
				String sql1 = "SELECT X_PRTY_FK,ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_SUPP_ACC_DDP"
				+ " WHERE trim(ROWID_OBJECT) IN (SELECT trim(X_SUPP_ACC_DDP_FK) FROM SUPPLIER_HUB.C_XR_SUPP_ACC_BU_REL"
				+ " WHERE ROWID_OBJECT='" + rowIdobject + "')";
				log.info("Inside sql1 line 3186" + sql1);
				buDDP = stmt.executeQuery("SELECT X_PRTY_FK,ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_SUPP_ACC_DDP"
						+ " WHERE trim(ROWID_OBJECT) IN (SELECT trim(X_SUPP_ACC_DDP_FK) FROM SUPPLIER_HUB.C_XR_SUPP_ACC_BU_REL"
						+ " WHERE ROWID_OBJECT='" + rowIdobject + "')");
				
				String prtyRowIdobject2 = "";
				
				log.info("Before while loop - 3193");
				while (buDDP.next()) {
					log.info("Inside while loop - 3195");
					log.info("PRTY_FK 3196 " + buDDP.getString("X_PRTY_FK"));
					log.info("ROWID_OBJECT 3197" + buDDP.getString("ROWID_OBJECT"));
					prtyRowIdobject1 = buDDP.getString("PRTY_FK");
				}
				log.info("After while loop - 3200");
				// Update PartyFK
				UpdatePrtyFkBUddp prtyFk = new UpdatePrtyFkBUddp();
				prtyFk.UpdatePrtyFkBUddpNew(rowIdobject, prtyRowIdobject1, stmt, stmt2);
				prtyFk.PreferredFlagDDP(rowIdobject, prtyRowIdobject1, stmt, tablename);
				
				
				// Soft delete for DNB Check
				stmtfk.close(); 
	            stmt2.close();
				stmt1.close(); 
				stmt.close();
				con.close();

			} catch (Exception e) {
				log.info("Exception in C_XO_SUPP_ACC_BU_DDP DB connection creation " + e);
			}
			finally{
				try { stmtfk.close();} catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { stmt2.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { stmt1.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
			}
		}	

//------------------------------------------------C_XO_DG_TRKNG-----------------------------------------------------------------------------
		log.info("Line 1810 TableName: " + tablename);
		if (tablename != null && tablename.equals("C_XO_DG_TRKNG")) {
			Set<String> setBOKey = baseObjectDataMap.keySet();
			if (setBOKey != null) {
				Iterator<String> itrBOKey = setBOKey.iterator();
				if (itrBOKey != null) {
					while (itrBOKey.hasNext()) {
						String stBOKey = (String) itrBOKey.next();
						Object objBO = (Object) baseObjectDataMap.get(stBOKey);
						if (objBO instanceof String) {
							String stValue = (String) objBO;
							if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
								rowIdobject = stValue;
								rowIdobject = rowIdobject.trim();
								log.info("C_XO_DG_TRKNG_POST_LOAD rowIdObject" + rowIdobject);
							}
						}
					}
				}
			}
			log.info("Begin DB C_XO_DG_TRKNG");
			Context ctx = null;
			Connection con = null;
			Statement stmt = null;
			try {
				ctx = new InitialContext();
				DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
				con = ds.getConnection();
				stmt = con.createStatement();
				
	            ResultSet dgComments =  stmt.executeQuery("SELECT X_DG_TEAM_CMNTS FROM SUPPLIER_HUB.C_XO_DG_TRKNG WHERE ROWID_OBJECT = '" + rowIdobject + "'");
	            
	            String dgTeamCmnts = null;
	            while (dgComments.next()) {
	                log.info("We are in While loop.");
	                dgTeamCmnts = dgComments.getString("X_DG_TEAM_CMNTS");
	                log.info("X_DG_TEAM_CMNTS -  " + dgTeamCmnts);
	            }
	            
	            stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_AVOS_TASK_LOGS SET X_DG_TEAM_CMTS = '" + dgTeamCmnts + "' WHERE X_PRTY_FK = "
	            		+ "(SELECT ONBD.X_PRTY_FK FROM SUPPLIER_HUB.C_XO_DG_TRKNG dg JOIN SUPPLIER_HUB.C_XO_SUPP_ONBRDNG_DTLS onbd ON dg.X_PRTY_ONBRD_FK =onbd.ROWID_OBJECT "
	            		+ "JOIN SUPPLIER_HUB.C_BO_PRTY prty ON onbd.X_PRTY_FK = prty.ROWID_OBJECT WHERE dg.ROWID_OBJECT = '" + rowIdobject +"')");
	            
	            log.info("DG Team Comments are updated.");
	            
	            stmt.close();
	            con.close();
	         }
			catch (Exception e) 
			{
				log.info("Exception in C_XO_DG_TRKNG DB connection creation " + e);
			}
			finally{
				try { stmt.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure stmt " + e); }
				try { con.close(); } catch (Exception e) { log.info("DNB_Exception in DB connection closure con " + e); }	
				}
			}
		}
		else { //Dynamic Import for Payment Details DDP and Accu Tech
			//---------------------------------------------------C_XO_PRTY_PMNT_DTLS--------------------------------------------------------------
			log.info("To add DBA name " + tablename);
				// Generating objectId for C_XO_PRTY_PMNT_DTLS
				if (tablename != null && tablename.equals("C_XO_PRTY_PMNT_DTLS")) {
					Set<String> setBOKey = baseObjectDataMap.keySet();
					if (setBOKey != null) {
						Iterator<String> itrBOKey = setBOKey.iterator();
						if (itrBOKey != null) {
							while (itrBOKey.hasNext()) {
								String stBOKey = (String) itrBOKey.next();
								Object objBO = (Object) baseObjectDataMap.get(stBOKey);
								if (objBO instanceof String) {
									String stValue = (String) objBO;
									if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
										rowIdobject = stValue;
										rowIdobject = rowIdobject.trim();
										log.info("C_XO_PRTY_PMNT_DTLS_POST_LOAD rowIdObject" + rowIdobject);
									}
									if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
										prtyRowIdobject = stValue;
										prtyRowIdobject = prtyRowIdobject.trim();
										log.info("C_XO_PRTY_PMNT_DTLS_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
									}
								}
							}
						}
					}
					//Ankit: Date:06/11/2025 
					log.info("Begin DB C_XO_PRTY_PMNT_DTLS Connection creation ");
				    DatabaseConnection dbHandler = new DatabaseConnection();
		            Statement stmt = null;

            try {
                stmt = dbHandler.createConnection();
            	GeneratePymntDtlsId pymntId = new GeneratePymntDtlsId();
				pymntId.GenerateUniquePmntDtlsId(rowIdobject, stmt, prtyRowIdobject);
            } catch (Exception e) {
            	log.info("Begin DB C_XO_PRTY_PMNT_DTLS Connection creation ");
            } finally {
                dbHandler.closeResources();
            }
			
					//End Ankit
				}
				
				//----------------------------------------------------------C_XO_PMNT_DTLS_ACCU_TECH-------------------------------------------------------------
				log.info("To add DBA name " + tablename);
				// Generating objectId for C_XO_PMNT_DTLS_ACCU_TECH
				if (tablename != null && tablename.equals("C_XO_PMNT_DTLS_ACCU_TECH")) {
					Set<String> setBOKey = baseObjectDataMap.keySet();
					if (setBOKey != null) {
						Iterator<String> itrBOKey = setBOKey.iterator();
						if (itrBOKey != null) {
							while (itrBOKey.hasNext()) {
								String stBOKey = (String) itrBOKey.next();
								Object objBO = (Object) baseObjectDataMap.get(stBOKey);
								if (objBO instanceof String) {
									String stValue = (String) objBO;
									if (stBOKey != null && stBOKey.equals("ROWID_OBJECT")) {
										rowIdobject = stValue;
										rowIdobject = rowIdobject.trim();
										log.info("C_XO_PMNT_DTLS_ACCU_TECH_POST_LOAD rowIdObject" + rowIdobject);
									}
									if (stBOKey != null && stBOKey.equals("X_PRTY_FK")) {
										prtyRowIdobject = stValue;
										prtyRowIdobject = prtyRowIdobject.trim();
										log.info("C_XO_PMNT_DTLS_ACCU_TECH_POST_LOAD Party ID rowIdObject" + prtyRowIdobject);
									}
								}
							}
						}
					}
					log.info("Begin DB C_XO_PMNT_DTLS_ACCU_TECH Connection creation ");
					//Ankit Date:06/11/2025
				/*
				 * **************
					Context ctx = null;
					Connection con = null;
					Statement stmt = null;
					try {
				
						ctx = new InitialContext();
						DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
						con = ds.getConnection();
						stmt = con.createStatement();
						// Generate unique paymentdetailid Accu Tech
						GeneratePymntDtlsIdAccuTech pymntId = new GeneratePymntDtlsIdAccuTech();
						pymntId.GenerateUniquePmntDtlsAccuTechId(rowIdobject, stmt, prtyRowIdobject);
						
						//Tax Registration Number (If tax Num is not null)
						
						// Soft delete for DNB Check
						stmt.close();
						con.close();
				
					} catch (Exception e) {
						log.info("Exception in C_XO_PMNT_DTLS_ACCU_TECH DB connection creation " + e);
					}
					finally{
						try { stmt.close(); } catch (Exception e) { log.info("C_XO_PMNT_DTLS_ACCU_TECH in DB connection closure stmt " + e); }
						try { con.close(); } catch (Exception e) { log.info("C_XO_PMNT_DTLS_ACCU_TECH in DB connection closure con " + e); }	
					}*******************
					*/
				DatabaseConnection dbHandler = new DatabaseConnection();
				Statement stmt = null;
				try {
					stmt = dbHandler.createConnection();
					GeneratePymntDtlsIdAccuTech pymntId = new GeneratePymntDtlsIdAccuTech();
					pymntId.GenerateUniquePmntDtlsAccuTechId(rowIdobject, stmt, prtyRowIdobject);
				} catch (Exception e) {
					log.info("Exception in C_XO_PMNT_DTLS_ACCU_TECH DB connection creation " + e);
				} finally {
					dbHandler.closeResources();
				}
//End Ankit Date:06/11/2025
					
				}
		}
	}	
}