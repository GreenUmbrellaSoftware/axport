package com.greenumbrellasoftware.axport;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import org.apache.ddlutils.model.Database;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

//import com.google.cloud.sql.jdbc.GoogleDataSource;

/**
 * Created with IntelliJ IDEA.
 * User: Kris
 * Date: 3/16/13
 * Time: 2:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class ExporterTest {

    private static final String DB = "rsta12";
    private static final String DB_USER = "root";
//    private static final String DB_PASSWORD = "password";

    @Test
    public void testCreateDatabaseFromAccessFile() throws Exception {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setDatabaseName(DB);
        ds.setUser(DB_USER);
//        ds.setPassword(DB_PASSWORD);

        Exporter.createDatabaseFromAccessFile(ds, new File("src/test/resources/RSTA_2012_League.mdb"));

    }

    @Test
    public void testCreateDatabaseSql() throws Exception {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setDatabaseName(DB);
        ds.setUser(DB_USER);

        String createSql = Exporter.createDatabaseDdlFromAccessFile(ds, new File("src/test/resources/RSTA_2012_League.mdb") ) ;
        Path createSqlPath = Paths.get("../swimreston/src/main/resources/createDatabase.sql");
        Files.deleteIfExists(createSqlPath);
        createSqlPath = Files.createFile(createSqlPath) ;
        BufferedWriter writer = Files.newBufferedWriter(createSqlPath, Charset.forName("UTF-8"));
        writer.append(createSql);
        writer.flush();

    }

    @Test
    public void testCreateDatabaseInsertStatementsFromAccessFile() throws Exception {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setDatabaseName(DB);
        ds.setUser(DB_USER);

        String insertSql  = Exporter.createDatabaseInsertStatementsFromAccessFile(ds, new File("src/test/resources/RSTA_2012_League.mdb") ) ;
        Path insertSqlPath = Paths.get("../swimreston/src/main/resources/insertData.sql");
        Files.deleteIfExists(insertSqlPath);
        insertSqlPath = Files.createFile(insertSqlPath) ;
        BufferedWriter writer = Files.newBufferedWriter(insertSqlPath, Charset.forName("UTF-8"));
        writer.append(insertSql);
        writer.flush();

    }


}
