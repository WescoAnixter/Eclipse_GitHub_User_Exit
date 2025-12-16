
package com.informatica.mdm.hubuserexit.postload;

import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.log4j.Logger;

public class GeneratePymntDtlsIdAccuTech {
	private static Logger log = Logger.getLogger(GeneratePymntDtlsIdAccuTech.class);

	public void GenerateUniquePmntDtlsAccuTechId(String rowIdobject, Statement stmt, String prtyRowIdobject) {
		ResultSet DBArs = null;
		ResultSet rs = null;
		try {
			DBArs = stmt.executeQuery("SELECT X_PMNT_DTLS_ID AS PMNT_DTLS_ID from SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH WHERE ROWID_OBJECT ="
							+ rowIdobject + "");
			
			String pmtId = null;
			while (DBArs.next()) {
				pmtId = DBArs.getString("PMNT_DTLS_ID");
				log.info("MAX_PMNT_DTLS_ID " + pmtId);
			}

			if (pmtId != null) {
				log.info("Updating X_LOC_NM_PRTY_FK prtyRowIdobject -> "+prtyRowIdobject);
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH SET X_LOC_NM_PRTY_FK= '"+prtyRowIdobject + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH_XREF SET X_LOC_NM_PRTY_FK= '"+prtyRowIdobject + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
				log.info("Updated X_LOC_NM_PRTY_FK");
			} 
			else {
				rs = stmt.executeQuery("select MAX(X_PMNT_DTLS_ID) AS MAX_PMNT_DTLS_ID from SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH");
				log.info("Conn334 " + rs);
				
				String maxPmtId = null;
				while (rs.next()) 
				{
					maxPmtId = rs.getString("MAX_PMNT_DTLS_ID");
					log.info("MAX_PMNT_DTLS_ID " + maxPmtId);
					if(maxPmtId != null)
					{
						log.info("MAX_PMNT_DTLS_ID is not NULL");
						int number = Integer.parseInt(maxPmtId.substring(1));
						number = number + 1;
						String numbers = Integer.toString(number);
						String initial = "A";
						initial = initial.concat(numbers);
						maxPmtId = initial;
						
						
						log.info("X_PMNT_DTLS_ID after increment " + maxPmtId);
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH SET X_PMNT_DTLS_ID = '" + maxPmtId +"', X_LOC_NM_PRTY_FK= '"+prtyRowIdobject + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH_XREF SET S_X_LOC_NM_PRTY_FK = '" + prtyRowIdobject + "', X_PMNT_DTLS_ID = '" + maxPmtId+"', X_LOC_NM_PRTY_FK= '"+prtyRowIdobject + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_ACCU_TECH_SUPP = 'Y' WHERE ROWID_OBJECT =" + prtyRowIdobject + "");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET X_ACCU_TECH_SUPP = 'Y' WHERE ROWID_OBJECT =" + prtyRowIdobject + "");
					}
					else
					{
						log.info("MAX_PMNT_DTLS_ID is NULL");
						String numbers = "A10001";
						maxPmtId = numbers;
						log.info("X_ACCU_TECH_SUPP accutech after increment " + maxPmtId);
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH SET X_PMNT_DTLS_ID = '" + maxPmtId
								+ "', X_LOC_NM_PRTY_FK= '"+prtyRowIdobject + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XO_PMNT_DTLS_ACCU_TECH_XREF SET X_PMNT_DTLS_ID = '" + maxPmtId
								+"', X_LOC_NM_PRTY_FK= '"+prtyRowIdobject + "' WHERE ROWID_OBJECT =" + rowIdobject + "");
						
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY SET X_ACCU_TECH_SUPP = 'Y' WHERE ROWID_OBJECT =" + prtyRowIdobject + "");
						stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_BO_PRTY_XREF SET X_ACCU_TECH_SUPP = 'Y' WHERE ROWID_OBJECT =" + prtyRowIdobject + "");
					}
					
				}
			}
			log.info("end GenerateUniquePmntDtlsAccuTechId ");
			
			DBArs.close();
			rs.close(); 
		} catch (Exception e) {
			log.info("Exception in GenerateUniquePmntDtlsAccuTechId" + e);
		}

	}
}

