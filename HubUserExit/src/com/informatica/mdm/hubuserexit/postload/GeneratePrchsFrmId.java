package com.informatica.mdm.hubuserexit.postload;

import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.log4j.Logger;

public class GeneratePrchsFrmId {
	private static Logger log = Logger.getLogger(GeneratePrchsFrmId.class);

	public void GenerateUniquePrchsFrmId(String rowIdobject, Statement stmt) {
		ResultSet DBArs = null;
		ResultSet rs = null;
		try {
			DBArs = stmt.executeQuery(
					"SELECT A.X_PRCHS_FRM_ID AS \"PRCHS_FRM_ID\" FROM SUPPLIER_HUB.C_XO_SUPP_ACC_DDP A JOIN SUPPLIER_HUB.C_BO_PRTY B ON A.X_PRTY_FK = B.ROWID_OBJECT WHERE B.X_PRTY_TYP != 'Wesco Legal Entity' AND A.ROWID_OBJECT ="
							+ rowIdobject + "");
			String pmtId = "";
			while (DBArs.next()) {
				pmtId = DBArs.getString("PRCHS_FRM_ID");
				log.info("MAX_PRCHS_FRM_ID " + pmtId);
			}

			if (pmtId != null) {
				log.info("Update Nothing here");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP SET X_DBA_NM_PRTY_FK=X_PRTY_FK WHERE ROWID_OBJECT =" + rowIdobject + "");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP_XREF SET S_X_DBA_NM_PRTY_FK=S_X_PRTY_FK,X_DBA_NM_PRTY_FK=X_PRTY_FK  WHERE ROWID_OBJECT =" + rowIdobject + "");
				
			} else {
				rs = stmt.executeQuery(
						"SELECT max(A.X_PRCHS_FRM_ID) AS \"MAX_PRCHS_FRM_ID\" FROM SUPPLIER_HUB.C_XO_SUPP_ACC_DDP A JOIN SUPPLIER_HUB.C_BO_PRTY B ON A.X_PRTY_FK = B.ROWID_OBJECT WHERE B.X_PRTY_TYP != 'Wesco Legal Entity'");
				log.info("Conn334 " + rs);
				String updatedPmtId = "";
				while (rs.next()) {
					String maxPmtId = rs.getString("MAX_PRCHS_FRM_ID");
					log.info("MAX_PRCHS_FRM_ID " + maxPmtId);
					if (maxPmtId != null) {
						log.info("MAX_PRCHS_FRM_ID is not NULL");
						int number = Integer.parseInt(maxPmtId);
						log.info("MAX_PRCHS_FRM_ID is " + number);
						number = number + 1;
						String numbers = Integer.toString(number);
						updatedPmtId = numbers;
						log.info("X_PRCHS_FRM_ID after increment " + updatedPmtId);
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP SET X_PRCHS_FRM_ID = '" + updatedPmtId
								+ "',X_DBA_NM_PRTY_FK=X_PRTY_FK WHERE ROWID_OBJECT =" + rowIdobject + "");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP_XREF SET X_PRCHS_FRM_ID = '"
								+ updatedPmtId + "',S_X_DBA_NM_PRTY_FK=S_X_PRTY_FK,X_DBA_NM_PRTY_FK=X_PRTY_FK  WHERE ROWID_OBJECT =" + rowIdobject + "");
					} else {
						log.info("MAX_PRCHS_FRM_ID is NULL");
						String numbers = "10000001";
						log.info("MAX_PRCHS_FRM_ID is " + numbers);
						updatedPmtId = numbers;
						log.info("X_PRCHS_FRM_ID after increment " + updatedPmtId);
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP SET X_PRCHS_FRM_ID = '" + updatedPmtId
								+ "',X_DBA_NM_PRTY_FK=X_PRTY_FK WHERE ROWID_OBJECT =" + rowIdobject + "");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP_XREF SET X_PRCHS_FRM_ID = '"
								+ updatedPmtId + "',S_X_DBA_NM_PRTY_FK=S_X_PRTY_FK,X_DBA_NM_PRTY_FK=X_PRTY_FK  WHERE ROWID_OBJECT =" + rowIdobject + "");
					}

				}
			} 
			
			ResultSet WLErs = null;
			ResultSet wrs = null;
			try 
			{
				log.info("We are in Try2");

				WLErs = stmt.executeQuery(
					"SELECT A.X_PRCHS_FRM_ID AS \"PRCHS_FRM_ID\" FROM SUPPLIER_HUB.C_XO_SUPP_ACC_DDP A JOIN SUPPLIER_HUB.C_BO_PRTY B ON A.X_PRTY_FK = B.ROWID_OBJECT WHERE B.X_PRTY_TYP = 'Wesco Legal Entity' AND A.ROWID_OBJECT ="
							+ rowIdobject + "");
				String prchsId = "";
				while (WLErs.next()) {
					prchsId = WLErs.getString("PRCHS_FRM_ID");
					log.info("MAX_PRCHS_FRM_ID " + prchsId);
			}

				if (prchsId != null) {
					log.info("Update Nothing here line 74");
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP SET X_DBA_NM_PRTY_FK=X_PRTY_FK WHERE ROWID_OBJECT =" + rowIdobject + "");
					stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP_XREF SET S_X_DBA_NM_PRTY_FK=S_X_PRTY_FK,X_DBA_NM_PRTY_FK=X_PRTY_FK  WHERE ROWID_OBJECT =" + rowIdobject + "");
			} 
				else {
					wrs = stmt.executeQuery(
						"SELECT max(A.X_PRCHS_FRM_ID) AS \"MAX_PRCHS_FRM_ID\" FROM SUPPLIER_HUB.C_XO_SUPP_ACC_DDP A JOIN SUPPLIER_HUB.C_BO_PRTY B ON A.X_PRTY_FK = B.ROWID_OBJECT WHERE B.X_PRTY_TYP = 'Wesco Legal Entity'");
					log.info("Conn334 " + wrs);
					String updatedPrchsId = "";
					while (wrs.next()) {
						String maxPrchsId = wrs.getString("MAX_PRCHS_FRM_ID");
						log.info("MAX_PRCHS_FRM_ID " + maxPrchsId);
						if (maxPrchsId != null) 
						{
							log.info("MAX_PRCHS_FRM_ID is not NULL");
							int number = Integer.parseInt(maxPrchsId);
							log.info("MAX_PRCHS_FRM_ID is " + number);
							number = number + 1;
							String numbers = Integer.toString(number);
							updatedPrchsId = numbers;
							log.info("X_PRCHS_FRM_ID after increment " + updatedPrchsId);
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP SET X_PRCHS_FRM_ID = '" + updatedPrchsId
									+ "',X_DBA_NM_PRTY_FK=X_PRTY_FK WHERE ROWID_OBJECT =" + rowIdobject + "");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP_XREF SET X_PRCHS_FRM_ID = '"
								+ updatedPrchsId +"',S_X_DBA_NM_PRTY_FK=S_X_PRTY_FK,X_DBA_NM_PRTY_FK=X_PRTY_FK  WHERE ROWID_OBJECT =" + rowIdobject + "");
						} 
						else 
						{
							log.info("MAX_PRCHS_FRM_ID is NULL");
							String numbers = "90000001";
							log.info("MAX_PRCHS_FRM_ID is " + numbers);
							updatedPrchsId = numbers;
							log.info("X_PRCHS_FRM_ID after increment " + updatedPrchsId);
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP SET X_PRCHS_FRM_ID = '" + updatedPrchsId
									+"',X_DBA_NM_PRTY_FK=X_PRTY_FK WHERE ROWID_OBJECT =" + rowIdobject + "");
							stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_DDP_XREF SET X_PRCHS_FRM_ID = '"
								+ updatedPrchsId + "',S_X_DBA_NM_PRTY_FK=S_X_PRTY_FK,X_DBA_NM_PRTY_FK=X_PRTY_FK  WHERE ROWID_OBJECT =" + rowIdobject + "");
							}

					}
			}
				
			log.info("end GenerateUniquePrchsFrmId ");
		}
		finally 
		{
			log.info("in Finally");
			if(DBArs != null){
				DBArs.close();
			}
			log.info("end DBArs ");
			if(rs != null){
				rs.close();
			}
			log.info("end rs ");
			if(WLErs != null){
				WLErs.close();
			}
			log.info("end WLErs ");
			if(wrs != null){
				wrs.close();
			}
			log.info("end wrs ");
								
	    } 
	}
		catch (Exception e) {
			log.info("Exception in GenerateUniquePrchsFrmId" + e);
		}

	}

	public void GenerateUniqueBUSuppAccId(String rowIdobjectDDP, String rowIdobject, Statement stmt, Statement stmt1, Statement stmt2) {
		ResultSet DBArs = null;
		ResultSet BUrs = null;
		ResultSet accDDP = null;
		ResultSet rs = null;
		try {
			log.info("--------------------------------- " +rowIdobjectDDP+"--------------------" +rowIdobject);
			// To Check if DDP's child id is null or already generated
			DBArs = stmt.executeQuery(
					"SELECT A.X_BU_PRCHS_ID AS \"BU_PRCHS_ID\""
							+ " from SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP A"
							+ " LEFT JOIN SUPPLIER_HUB.C_XR_SUPP_ACC_BU_REL B ON A.ROWID_OBJECT = B.X_SUPP_ACC_BU_DDP_FK"
							+ " WHERE B.ROWID_OBJECT =" + rowIdobjectDDP + "");
			String BUprchsId = "";
			log.info("BU_PRCHS_ID before ");
			while (DBArs.next()) {
				BUprchsId = DBArs.getString("BU_PRCHS_ID");
				log.info("BU_PRCHS_ID after");
			}	
			log.info("BU_PRCHS_ID " + BUprchsId);
			if (BUprchsId != null) {
				log.info("Update 1 here");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP SET X_PMNT_PRTY_FK='"+rowIdobject
						+"' WHERE ROWID_OBJECT =" + rowIdobjectDDP + "");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP_XREF SET S_X_PMNT_PRTY_FK='"+rowIdobject
						+"',X_PMNT_PRTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF cbpx WHERE ROWID_OBJECT ='"
						+rowIdobject+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY ) WHERE ROWID_OBJECT =" + rowIdobjectDDP + "");
			} else {
				// To Get DDP account ID based on load in DDP's child
				BUrs = stmt1.executeQuery(
						"SELECT A.X_BU_PRCHS_ID AS \"BU_PRCHS_ID\",C.X_PRCHS_FRM_ID AS \"PRCHS_FRM_ID\",C.ROWID_OBJECT AS \"DDP_ROWID_OBJECT\",B.X_SUPP_ACC_BU_DDP_FK AS \"ACC_ROWID_OBJECT\""
								+ " FROM SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP A"
								+ " LEFT JOIN SUPPLIER_HUB.C_XR_SUPP_ACC_BU_REL B ON A.ROWID_OBJECT = B.X_SUPP_ACC_BU_DDP_FK"
								+ " LEFT JOIN SUPPLIER_HUB.C_XO_SUPP_ACC_DDP C ON B.X_SUPP_ACC_DDP_FK = C.ROWID_OBJECT"
								+ " LEFT JOIN SUPPLIER_HUB.C_BO_PRTY D ON D.ROWID_OBJECT=C.X_PRTY_FK WHERE D.ROWID_OBJECT = "
								+ rowIdobject + " AND B.ROWID_OBJECT = " + rowIdobjectDDP + "");
				String prchsFrmId = "";
				String DDPObjectId = "";
				String accObjectId = "";
				while (BUrs.next()) {
					prchsFrmId = BUrs.getString("PRCHS_FRM_ID");
					DDPObjectId = BUrs.getString("DDP_ROWID_OBJECT");
					accObjectId = BUrs.getString("ACC_ROWID_OBJECT");
					log.info("BU_PRCHS_ID " + prchsFrmId);
					log.info("DDP_ROWID_OBJECT " + DDPObjectId);
					log.info("ACC_ROWID_OBJECT " + accObjectId);
				}

				// To Get Business Unit and Currency Type
				accDDP = stmt.executeQuery(
						" Select C.X_BSNS_UNIT AS BU_BSNS_UNIT,C.X_PO_CRNCY AS BU_PO_CRNCY from SUPPLIER_HUB.C_XR_SUPP_ACC_BU_REL A"
								+ " LEFT JOIN SUPPLIER_HUB.C_XO_SUPP_ACC_DDP B ON A.X_SUPP_ACC_DDP_FK = B.ROWID_OBJECT"
								+ " LEFT JOIN SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP C ON A.X_SUPP_ACC_BU_DDP_FK = C.ROWID_OBJECT"
								+ " WHERE A.X_SUPP_ACC_BU_DDP_FK =" + accObjectId + "");
				String BU_BSNS_UNIT = "";
				String BU_PO_CRNCY = "";
				while (accDDP.next()) {
					BU_BSNS_UNIT = accDDP.getString("BU_BSNS_UNIT");
					BU_PO_CRNCY = accDDP.getString("BU_PO_CRNCY");
					log.info("BU_BSNS_UNIT " + BU_BSNS_UNIT);
					log.info("BU_PO_CRNCY " + BU_PO_CRNCY);
				}

				// To Get max of DDP child based on DDP
				rs = stmt2.executeQuery(
						"Select MAX(C.X_BU_PRCHS_ID) AS \"MAX_X_BU_PRCHS_ID\" from SUPPLIER_HUB.C_XR_SUPP_ACC_BU_REL A"
								+ " LEFT JOIN SUPPLIER_HUB.C_XO_SUPP_ACC_DDP B ON A.X_SUPP_ACC_DDP_FK = B.ROWID_OBJECT"
								+ " LEFT JOIN SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP C ON A.X_SUPP_ACC_BU_DDP_FK = C.ROWID_OBJECT"
								+ " WHERE A.X_SUPP_ACC_DDP_FK =" + DDPObjectId + "");
				log.info("Conn334 " + rs);
				String updatedBUId = "";
				String numbers = "";
				while (rs.next()) {
					String maxBUId = rs.getString("MAX_X_BU_PRCHS_ID");
					log.info("MAX_BU_PRCHS_ID " + maxBUId);
					if (maxBUId != null) {
						log.info("MAX_BU_PRCHS_ID is not NULL");

						int number = Integer.parseInt(maxBUId.substring(8, 10));
						number = number + 1;
						numbers = Integer.toString(number);
						if (number < 10) {
							log.info("Less then 10 ");
							numbers = "0".concat(numbers);
						}
						log.info("1st after increment " + numbers);
						updatedBUId = prchsFrmId.concat(numbers);
						log.info("1st after increment " + updatedBUId);
						updatedBUId = updatedBUId.concat(BU_BSNS_UNIT);
						log.info("2nd after increment " + updatedBUId);
						updatedBUId = updatedBUId.concat(BU_PO_CRNCY);
						log.info("3rd after increment " + updatedBUId);
						log.info("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP SET X_BU_PRCHS_ID = '"
								+ updatedBUId + "',X_PMNT_PRTY_FK='"+rowIdobject+"' WHERE ROWID_OBJECT =" + accObjectId + "");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP SET X_BU_PRCHS_ID = '"
								+ updatedBUId + "',X_PMNT_PRTY_FK='"+rowIdobject+"' WHERE ROWID_OBJECT =" + accObjectId + "");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP_XREF SET X_BU_PRCHS_ID = '"
								+ updatedBUId + "',S_X_PMNT_PRTY_FK='"+rowIdobject+"',X_PMNT_PRTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF cbpx WHERE ROWID_OBJECT ='"+rowIdobject+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY ) WHERE ROWID_OBJECT =" + accObjectId + "");
					} else {
						log.info(" Else 226 MAX_BU_PRCHS_ID is NULL");
						String initial = "01";
						log.info("1st after increment " + updatedBUId);
						updatedBUId = prchsFrmId.concat(initial);
						log.info("1st after increment " + updatedBUId);
						updatedBUId = updatedBUId.concat(BU_BSNS_UNIT);
						log.info("2nd after increment " + updatedBUId);
						updatedBUId = updatedBUId.concat(BU_PO_CRNCY);
						log.info("3rd after increment " + updatedBUId);
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP SET X_BU_PRCHS_ID ='"
								+ updatedBUId + "',X_PMNT_PRTY_FK='"+rowIdobject+"' WHERE ROWID_OBJECT =" + accObjectId + "");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_BU_DDP_XREF SET X_BU_PRCHS_ID = '"
								+ updatedBUId + "',S_X_PMNT_PRTY_FK='"+rowIdobject+"',X_PMNT_PRTY_FK=(SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF cbpx WHERE ROWID_OBJECT ='"+rowIdobject+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY ) WHERE ROWID_OBJECT =" + accObjectId + "");
					}

				}
				rs.close();
				accDDP.close();
				log.info("end GenerateUniquePrchsFrmId1 ");
				BUrs.close();
			}
			log.info("end GenerateUniquePrchsFrmId ");

			DBArs.close();
		}
		catch (Exception e) {
			log.info("Exception in GenerateUniquePrchsFrmId" + e);
		}

	}

}
