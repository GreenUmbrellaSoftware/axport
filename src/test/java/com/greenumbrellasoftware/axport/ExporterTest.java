package com.greenumbrellasoftware.axport;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.File;

//import com.google.cloud.sql.jdbc.GoogleDataSource;

/**
 * Created with IntelliJ IDEA.
 * User: Kris
 * Date: 3/16/13
 * Time: 2:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class ExporterTest {

    private static final String DB = "rstaDB";
    private static final String DB_USER = "rstaAdmin";
    private static final String DB_PASSWORD = "password";

    @Test
    public void testCreateDatabaseFromAccessFile() throws Exception {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setDatabaseName(DB);
        ds.setUser(DB_USER);
        ds.setPassword(DB_PASSWORD);

        Exporter.createDatabaseFromAccessFile(ds, new File("src/test/resources/RSTA_2012_League.mdb"));

    }


}
