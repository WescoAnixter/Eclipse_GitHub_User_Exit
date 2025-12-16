package com.informatica.mdm.hubuserexit.postload;

import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.log4j.Logger;

public class PendingDeleteUpdate {
	private static Logger log = Logger.getLogger(PendingDeleteUpdate.class);
	
	
	//To delete pending record for DnB
	public void pendingUpdateDnB(String prtyRowIdobject, Statement stmt) {
		//ResultSet rs = null;
		ResultSet rs2 = null;
		Integer count = 0;
		try {
			log.info("DNB_start pendingUpdateDnB ");
			/*rs = stmt.executeQuery(
					"SELECT ROWID_OBJECT AS DNB_ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_ADDR_DNB_VLDN WHERE X_PRTY_FK ="
							+ prtyRowIdobject + "");*/
			
			rs2 = stmt.executeQuery(
					"SELECT ROWID_OBJECT AS DNB_ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_ADDR_DNB_VLDN WHERE X_PRTY_FK ="
							+ prtyRowIdobject + "");
			
			while(rs2.next()) {
				log.info("Count - " + count);
				count = count + 1 ;
			}
			log.info("DNB_Before while in pendingUpdateDnB ");
			//log.info("Length of ResulSet - " + rs.getFetchSize());
			//String DnBCheckValue = "";
			if(count > 0) {
				log.info("Inside IF line 34");
				stmt.executeUpdate(
						"UPDATE SUPPLIER_HUB.C_XO_ADDR_DNB_VLDN SET HUB_STATE_IND ='-1' WHERE X_PRTY_FK ="
								+ prtyRowIdobject + " AND (X_MATCH IS NULL OR X_MATCH !='Y')");
				stmt.executeUpdate(
						"UPDATE SUPPLIER_HUB.C_XO_ADDR_DNB_VLDN_XREF SET HUB_STATE_IND ='-1' WHERE X_PRTY_FK ="
								+ prtyRowIdobject + " AND (X_MATCH IS NULL OR X_MATCH !='Y')");
			}
			/*while (rs.next()) {
				DnBCheckValue = rs.getString("DNB_ROWID_OBJECT");
				if (DnBCheckValue != null) {
					log.info("DNB_Before update in pendingUpdateDnB ");
					stmt.executeUpdate(
							"UPDATE SUPPLIER_HUB.C_XO_ADDR_DNB_VLDN SET HUB_STATE_IND ='-1' WHERE X_PRTY_FK ="
									+ prtyRowIdobject + " AND (X_MATCH IS NULL OR X_MATCH !='Y')");
					stmt.executeUpdate(
							"UPDATE SUPPLIER_HUB.C_XO_ADDR_DNB_VLDN_XREF SET HUB_STATE_IND ='-1' WHERE X_PRTY_FK ="
									+ prtyRowIdobject + " AND (X_MATCH IS NULL OR X_MATCH !='Y')");
				}
			}*/
			log.info("DNB_end pendingUpdateDnB ");
			//rs.close();
			rs2.close();
		} catch (Exception e) {
			log.info("DNB_Exception in pendingUpdateDnB " + e);
		}

	}
	
	//To delete pending record for VC
	public void pendingUpdateVC(String prtyRowIdobject, Statement stmt) {
		ResultSet rs = null;
		try {
			log.info("VC_start pendingUpdateExistingSupp ");
			rs = stmt.executeQuery(
					"SELECT ROWID_OBJECT AS VC_ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_VSUL_CMPLNC_VLDN WHERE X_PRTY_FK ="
							+ prtyRowIdobject + "");
			log.info("VC_Before while in pendingUpdateDnB ");
			String VCValue = "";
			while (rs.next()) {
				VCValue = rs.getString("VC_ROWID_OBJECT");

				if (VCValue != null) {
					log.info("VC_Before update in pendingUpdateExistingSupp ");
					stmt.executeUpdate(
							"UPDATE SUPPLIER_HUB.C_XO_VSUL_CMPLNC_VLDN SET HUB_STATE_IND ='-1' WHERE X_PRTY_FK ="
									+ prtyRowIdobject + " AND (X_SELECT IS NULL OR X_SELECT !='Y')");
					stmt.executeUpdate(
							"UPDATE SUPPLIER_HUB.C_XO_VSUL_CMPLNC_VLDN_XREF SET HUB_STATE_IND ='-1' WHERE X_PRTY_FK ="
									+ prtyRowIdobject + " AND (X_SELECT IS NULL OR X_SELECT !='Y')");
				}
			}
			log.info("VC_end pendingUpdateExistingSupp ");
			rs.close();
		} catch (Exception e) {
			log.info("VC_Exception in pendingUpdateExistingSupp " + e);
		}

	}
	
	//To delete pending record for Existing Supplier
		public void pendingUpdateExistingSupp(String prtyRowIdobject, Statement stmt) {
			ResultSet rs = null;
			try {
				log.info("ExistSupp_start pendingUpdateExistingSupp ");
				rs = stmt.executeQuery(
						"SELECT ROWID_OBJECT AS EXSUPP_ROWID_OBJECT FROM SUPPLIER_HUB.C_XO_EX_SUPP_REC WHERE X_PRTY_FK ="
								+ prtyRowIdobject + "");
				log.info("ExistSupp_Before while in pendingUpdateDnB ");
				String EXSUPPCheckValue = "";
				while (rs.next()) {
					EXSUPPCheckValue = rs.getString("EXSUPP_ROWID_OBJECT");
					if (EXSUPPCheckValue != null) {
						log.info("ExistSupp_Before update in pendingUpdateExistingSupp ");
						stmt.executeUpdate(
								"UPDATE SUPPLIER_HUB.C_XO_EX_SUPP_REC SET HUB_STATE_IND ='-1' WHERE X_PRTY_FK ="
										+ prtyRowIdobject + "");
						stmt.executeUpdate(
								"UPDATE SUPPLIER_HUB.C_XO_EX_SUPP_REC_XREF SET HUB_STATE_IND ='-1' WHERE X_PRTY_FK ="
										+ prtyRowIdobject + "");
					}
				}
				log.info("ExistSupp_end pendingUpdateExistingSupp ");
				rs.close();
			} catch (Exception e) {
				log.info("ExistSupp_Exception in pendingUpdateExistingSupp " + e);
			}

		}
	
}
