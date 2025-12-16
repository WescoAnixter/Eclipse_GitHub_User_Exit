package com.informatica.mdm.hubuserexit.postload;

import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.log4j.Logger;

public class GenerateLocId {

	private static Logger log = Logger.getLogger(GenerateLocId.class);
	
	public void GenerateUniqueLocID(String rowIdobject, Statement stmt,String prtyRowIdobject) {
		log.info("prtyRowIdobject: "+prtyRowIdobject);
		log.info("rowIdobject: "+rowIdobject);
		ResultSet DBArs = null;
		ResultSet rs = null;
		ResultSet rs1 = null;
		try {
			
			log.info("RowidObject ----> " + rowIdobject);
			DBArs = stmt
					.executeQuery("select X_ASSOC_LOC_NM AS ASSOC_LOC_NM,X_LOC_ID as LocID from SUPPLIER_HUB.C_XR_BNK_ADDR_REL WHERE ROWID_OBJECT ="
							+ rowIdobject + "");
			String AssocLocNm = "";
			String LocID = "";
			while (DBArs.next()) {
				AssocLocNm = DBArs.getString("ASSOC_LOC_NM");
				log.info("X_ASSOC_LOC_NM ---> " + AssocLocNm);
				//LocID = DBArs.getString("LocID");
				log.info("X_ASSOC_LOC_NM ---> " + LocID);
			}
				
			rs = stmt.executeQuery("SELECT X_STS AS STS FROM SUPPLIER_HUB.C_BO_PSTL_ADDR WHERE ROWID_OBJECT = '"+ AssocLocNm + "'");
			String status = "";
			
			while (rs.next()) {
				status = rs.getString("STS");
				log.info("Status ---> " + status);
			}
			
			rs1 = stmt.executeQuery("SELECT X_PRTY_FK as prtyfk FROM SUPPLIER_HUB.C_XT_LOC_NM WHERE X_LOC_ID ='"+ AssocLocNm + "'");
			String prtyfk = "";
			
			while (rs1.next()) {
				prtyfk = rs1.getString("prtyfk");
				log.info("prtyfk ---> " + prtyfk);
			}
			
			/*stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XR_BNK_ADDR_REL SET X_ADDR_STS = '" + status +"',X_LOC_NM_PRTY_FK='" +prtyfk+"' WHERE ROWID_OBJECT = '" + rowIdobject + "'");
			stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XR_BNK_ADDR_REL_XREF SET X_ADDR_STS = '" + status +"',, S_X_LOC_NM_PRTY_FK='"+ prtyfk+ 
						"', X_LOC_NM_PRTY_FK= (SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF WHERE ROWID_OBJECT ='"+prtyfk+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY ) WHERE ROWID_OBJECT = '" + rowIdobject + "'");*/
			log.info("Sub Query updated. Status "+status+" prtyfk: "+prtyfk);

			if(AssocLocNm != null){
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XR_BNK_ADDR_REL SET X_ADDR_STS = '" + status +"', X_LOC_ID = '" + AssocLocNm +"', X_LOC_NM_PRTY_FK='"+ prtyfk+ "' WHERE ROWID_OBJECT = '" + rowIdobject + "'");
				stmt.executeUpdate("UPDATE SUPPLIER_HUB.C_XR_BNK_ADDR_REL_XREF SET X_ADDR_STS = '" + status +"', S_X_ADDR_STS = '" + status +"', X_LOC_ID = '" + AssocLocNm+"', S_X_LOC_NM_PRTY_FK='"+ prtyfk+ 
						"', X_LOC_NM_PRTY_FK= (SELECT ROWID_XREF FROM SUPPLIER_HUB.C_BO_PRTY_XREF WHERE ROWID_OBJECT ='"+prtyfk+"'ORDER BY LAST_UPDATE_DATE ASC FETCH FIRST 1 ROWS ONLY ) WHERE ROWID_OBJECT = '" + rowIdobject + "'");
				log.info("X_ASSOC_LOC_NM updated.");
		
			}
			else{
				log.info("X_ASSOC_LOC_NM is NULL.");
			}
			log.info("end GenerateUniqueLocID ");
		} 
		catch (Exception e) {
			log.info("Exception in GenerateUniqueLocID" + e);
		}
	}
}
