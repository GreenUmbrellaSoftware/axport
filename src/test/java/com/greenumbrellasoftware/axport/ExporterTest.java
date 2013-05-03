package com.greenumbrellasoftware.axport;

import static junit.framework.Assert.*;

//import com.google.cloud.sql.jdbc.GoogleDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

/**
 * Created with IntelliJ IDEA.
 * User: Kris
 * Date: 3/16/13
 * Time: 2:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class ExporterTest {

    private static final Log LOG = LogFactory.getLog(ExporterTest.class);
    private static final String DB = "rstaDB";
    private static final String DB_USER = "rstaAdmin";
    private static final String DB_PASSWORD = "password";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/" + DB;

    @Test
    public void testExportSchemaToFile() throws Exception {
        Database database = Exporter.exportSchema(DB, new File("src/test/resources/RSTA_2012_League.mdb"));
//        File schemaFile = File.createTempFile(String.format("%s-", ExporterTest.class.getSimpleName()), ".xml");
        File schemaFile = new File("src/test/resources/schema.xml");
        LOG.debug(String.format("Writing schema file to %s", schemaFile.getAbsolutePath()));
        assertFalse(schemaFile.length() > 0);
        new DatabaseIO().write(database, schemaFile.getAbsolutePath());
        assertTrue(schemaFile.length() > 0);
    }

    @Test
    public void testExportSchemaToMysql() throws Exception {
        Database database = Exporter.exportSchema(DB, new File("src/test/resources/RSTA_2012_League.mdb"));

//        GoogleDataSource ds = new GoogleDataSource();
        MysqlDataSource ds = new MysqlDataSource();
        ds.setDatabaseName(DB);
        ds.setUser(DB_USER);
        ds.setPassword(DB_PASSWORD);

        Connection con = ds.getConnection();
        DatabaseMetaData md = con.getMetaData();
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

    @Test
    public void testCreateDatabaseFromAccessFile() throws Exception {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setDatabaseName(DB);
        ds.setUser(DB_USER);
        ds.setPassword(DB_PASSWORD);

        Exporter.createDatabaseFromAccessFile(ds, new File("src/test/resources/RSTA_2012_League.mdb"));

    }

    @Test
    public void testExportDataToFile() throws Exception {

        File outputFile = new File("src/test/resources/data.xml");

        if (outputFile.exists()) {
            outputFile.delete();
        }

        assertFalse(outputFile.exists());

        Exporter.exportDataToFile(new File("src/test/resources/RSTA_2012_League.mdb"), outputFile);

        assertTrue(outputFile.exists());

    }


    @Test
    public void testImportDataFile() throws Exception {

        File schemaFile = new File("src/test/resources/schema.xml");
        File dataFile = new File("src/test/resources/data.xml");

        org.apache.commons.dbcp.BasicDataSource ds = new org.apache.commons.dbcp.BasicDataSource();
        ds.setDriverClassName(com.mysql.jdbc.Driver.class.getName());
        ds.setUsername(DB_USER);
        ds.setPassword(DB_PASSWORD);
        ds.setUrl(DB_URL);

        Exporter.importDataFile(dataFile, schemaFile, ds);


    }

}
