package com.greenumbrellasoftware.axport;

import java.io.File;

import org.junit.Test;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

/**
 * Test to exercise the static methods in the Exporter class. The most important
 * test is testCreateDatabaseFromAccessFile which will create the complete mysql
 * data structure and data in a mysql database (running at the default settings
 * and with the configured test username and password).
 * 
 */
public class ExporterTest {

	private static final String YEAR = "12";

	private static final String DB = "rsta" + YEAR;
	private static final String DB_USER = "root";
	private static final String DB_PASSWORD = "password";
	private static final String TEAM_MANAGER_FILENAME = "src/test/resources/RSTA_20"
			+ YEAR + "_League.mdb";

	@Test
	public void testCreateDatabaseFromAccessFile() throws Exception {
		MysqlDataSource ds = new MysqlDataSource();
		ds.setDatabaseName(DB);
		ds.setUser(DB_USER);
		ds.setPassword(DB_PASSWORD);

		Exporter.createDatabaseFromAccessFile(ds, new File(
				TEAM_MANAGER_FILENAME));

	}

	// @Test
	// public void testCreateDatabaseSql() throws Exception {
	// MysqlDataSource ds = new MysqlDataSource();
	// ds.setDatabaseName(DB);
	// ds.setUser(DB_USER);
	// ds.setPassword(DB_PASSWORD);
	//
	// String createSql = Exporter.createDatabaseDdlFromAccessFile(ds,
	// new File(TEAM_MANAGER_FILENAME));
	// Path createSqlPath = Paths
	// .get("../swimreston/src/main/resources/createDatabase.sql");
	// Files.deleteIfExists(createSqlPath);
	// createSqlPath = Files.createFile(createSqlPath);
	// BufferedWriter writer = Files.newBufferedWriter(createSqlPath,
	// Charset.forName("UTF-8"));
	// writer.append(createSql);
	// writer.flush();
	//
	// }
	//
	// @Test
	// public void testCreateDatabaseInsertStatementsFromAccessFile()
	// throws Exception {
	// MysqlDataSource ds = new MysqlDataSource();
	// ds.setDatabaseName(DB);
	// ds.setUser(DB_USER);
	// ds.setPassword(DB_PASSWORD);
	//
	// String insertSql = Exporter
	// .createDatabaseInsertStatementsFromAccessFile(ds, new File(
	// TEAM_MANAGER_FILENAME));
	// Path insertSqlPath = Paths
	// .get("../swimreston/src/main/resources/insertData.sql");
	// Files.deleteIfExists(insertSqlPath);
	// insertSqlPath = Files.createFile(insertSqlPath);
	// BufferedWriter writer = Files.newBufferedWriter(insertSqlPath,
	// Charset.forName("UTF-8"));
	// writer.append(insertSql);
	// writer.flush();
	//
	// }

}
