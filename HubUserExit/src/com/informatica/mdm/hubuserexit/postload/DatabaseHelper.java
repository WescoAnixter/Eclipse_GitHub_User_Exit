package com.informatica.mdm.hubuserexit.postload;

import java.sql.*;
import javax.naming.*;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

public class DatabaseHelper {

    private static final Logger log = Logger.getLogger(DatabaseHelper.class);

    public static void performDBOperation(String prtyRowIdobject, String type) {
        Context ctx = null;
        Connection con = null;
        Statement stmt = null;

        try {
            log.info(type + "_Begin DB Connection creation");

            ctx = new InitialContext();
            DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
            con = ds.getConnection();
            stmt = con.createStatement();

            log.info(type + "_Before PendingDeleteUpdate");

            PendingDeleteUpdate pdu = new PendingDeleteUpdate();
            if ("DNB".equalsIgnoreCase(type)) {
                pdu.pendingUpdateDnB(prtyRowIdobject, stmt);
            } else if ("VC".equalsIgnoreCase(type)) {
                pdu.pendingUpdateVC(prtyRowIdobject, stmt);
            }

            log.info(type + "_End DB Connection successfully");

        } catch (Exception e) {
            log.error(type + "_Exception in DB operation: ", e);
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (Exception e) {
                log.error(type + "_Exception in stmt close: ", e);
            }
            try {
                if (con != null) con.close();
            } catch (Exception e) {
                log.error(type + "_Exception in con close: ", e);
            }
        }
    }
}