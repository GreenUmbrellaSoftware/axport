package com.greenumbrellasoftware.axport;

import static junit.framework.Assert.*;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created with IntelliJ IDEA.
 * User: Kris
 * Date: 3/16/13
 * Time: 2:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class ExporterTest {

    private static final Log LOG = LogFactory.getLog(ExporterTest.class);
    private static final String DB =   "rstaDB" ;
    private static final String DB_USER =   "rstaAdmin" ;
    private static final String DB_PASSWORD =   "password" ;

    @Test
    public void testExportSchema() throws Exception {
        Database database = Exporter.exportSchema(new File("src/test/resources/RSTA_2012_League.mdb"));
        File schemaFile = File.createTempFile(String.format("%s-", ExporterTest.class.getSimpleName()), ".xml");
        LOG.debug(String.format("Writing schema file to %s", schemaFile.getAbsolutePath()));
        assertFalse(schemaFile.length() > 0);
        new DatabaseIO().write(database, schemaFile.getAbsolutePath());
        assertTrue(schemaFile.length() > 0);
    }

    @Test
    public void testExportSchemaToMysql() throws Exception {
        Database database = Exporter.exportSchema(new File("src/test/resources/RSTA_2012_League.mdb"));

        MysqlDataSource ds = new MysqlDataSource();
        ds.setDatabaseName(DB);
        ds.setUser(DB_USER);
        ds.setPassword(DB_PASSWORD);

        Connection con = ds.getConnection();
        DatabaseMetaData md =  con.getMetaData() ;
        ResultSet rs = md.getTables(null, null, "%", null);
        while (rs.next()) {
            LOG.debug(rs.getString("TABLE_NAME"));
            fail("There should be no tables at this point.");
        }


        Platform platform = PlatformFactory.createNewPlatformInstance(ds);
        platform.createTables(database, false, false);


        rs.close();
        con.close();


    }

}
