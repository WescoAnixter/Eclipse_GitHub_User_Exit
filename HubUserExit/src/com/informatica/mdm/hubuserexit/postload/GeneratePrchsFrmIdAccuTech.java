package com.informatica.mdm.hubuserexit.postload;

import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.log4j.Logger;

public class GeneratePrchsFrmIdAccuTech {
	private static Logger log = Logger.getLogger(GeneratePrchsFrmIdAccuTech.class);

	public void GenerateUniquePrchsFrmIdAccuTech(String rowIdobject, Statement stmt) {
		ResultSet DBArs = null;
		ResultSet rs = null;
		try {
			DBArs = stmt.executeQuery("SELECT X_BU_PRCHS_ID AS PRCHS_FRM_ID FROM SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH WHERE ROWID_OBJECT ='"
							+ rowIdobject + "'");
			String pmntId = "";
			while (DBArs.next()) {
				pmntId = DBArs.getString("PRCHS_FRM_ID");
				log.info("ACCUTECH PRCHS_FRM_ID " + pmntId);
			}

			if (pmntId != null) {
				log.info("ACCUTECH Update Nothing here");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH SET X_DBA_NM_PRTY_FK=X_PRTY_FK WHERE ROWID_OBJECT =" + rowIdobject + "");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH_XREF SET S_X_DBA_NM_PRTY_FK=S_X_PRTY_FK,X_DBA_NM_PRTY_FK=X_PRTY_FK  WHERE ROWID_OBJECT =" + rowIdobject + "");
				
			} else {
				rs = stmt.executeQuery("SELECT max(X_BU_PRCHS_ID) AS MAX_PRCHS_FRM_ID FROM SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH");
				log.info("Conn31 " + rs);
				String updatedPmntId = "";
				while (rs.next()) {
					String maxPmntId = rs.getString("MAX_PRCHS_FRM_ID");
					log.info("MAX_PRCHS_FRM_ID " + maxPmntId);
					if (maxPmntId != null) {
						log.info("MAX_PRCHS_FRM_ID_AT is not NULL");
						int number = Integer.parseInt(maxPmntId);
						log.info("MAX_PRCHS_FRM_ID_AT is " + number);
						number = number + 1;
						String numbers = Integer.toString(number);
						updatedPmntId = numbers;
						log.info("X_BU_PRCHS_ID after increment " + updatedPmntId);
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH SET X_BU_PRCHS_ID = '" + updatedPmntId
								+ "',X_DBA_NM_PRTY_FK=X_PRTY_FK WHERE ROWID_OBJECT =" + rowIdobject + "");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH_XREF SET X_BU_PRCHS_ID = '"
								+ updatedPmntId + "',S_X_DBA_NM_PRTY_FK=S_X_PRTY_FK,X_DBA_NM_PRTY_FK=X_PRTY_FK  WHERE ROWID_OBJECT =" + rowIdobject + "");
					} else {
						log.info("MAX_PRCHS_FRM_ID_AT is NULL");
						String numbers = "40000001";
						log.info("MAX_PRCHS_FRM_ID is " + numbers);
						updatedPmntId = numbers;
						log.info("X_BU_PRCHS_ID after increment " + updatedPmntId);
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH SET X_BU_PRCHS_ID = '" + updatedPmntId
								+ "',X_DBA_NM_PRTY_FK=X_PRTY_FK WHERE ROWID_OBJECT =" + rowIdobject + "");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_SUPP_ACC_ACCU_TECH_XREF SET X_BU_PRCHS_ID = '"
								+ updatedPmntId + "',S_X_DBA_NM_PRTY_FK=S_X_PRTY_FK,X_DBA_NM_PRTY_FK=X_PRTY_FK  WHERE ROWID_OBJECT =" + rowIdobject + "");
					}

				}
			} 
			
	}
		catch (Exception e) {
			log.info("Exception in GenerateUniquePrchsFrmIdAccuTech" + e);
		}

	}

}
