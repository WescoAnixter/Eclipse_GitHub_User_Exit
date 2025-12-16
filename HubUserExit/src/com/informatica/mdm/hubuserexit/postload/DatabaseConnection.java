package com.informatica.mdm.hubuserexit.postload;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

public class DatabaseConnection {
    
    private static final Logger log = Logger.getLogger(DatabaseConnection.class);

    private Context ctx = null;
    private Connection con = null;
    private Statement stmt = null;

    public Statement createConnection() throws Exception {
        log.info("Creating DB Connection...");
        ctx = new InitialContext();
        DataSource ds = (DataSource) ctx.lookup("java:jboss/datasources/jdbc/siperian-orcl-supplier_hub-ds");
        con = ds.getConnection();
        stmt = con.createStatement();
        return stmt;
    }
    
    

    public void closeResources() {
        try {
            if (stmt != null) {
                stmt.close();
                log.info("Statement closed successfully.");
            }
        } catch (Exception e) {
            log.info("DNB_Exception in DB connection closure stmt " + e);
        }

        try {
            if (con != null) {
                con.close();
                log.info("Connection closed successfully.");
            }
        } catch (Exception e) {
            log.info("DNB_Exception in DB connection closure con " + e);
        }
    }
}