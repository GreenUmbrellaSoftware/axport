package com.greenumbrellasoftware.axport;

import com.healthmarketscience.jackcess.*;
import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.io.DataWriter;
import org.apache.ddlutils.task.DdlToDatabaseTask;
import org.apache.ddlutils.task.WriteDataToDatabaseCommand;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

import static org.apache.commons.lang.StringUtils.remove;

/**
 * Created with IntelliJ IDEA.
 * User: Kris Gholson
 * Date: 3/16/13
 * Time: 1:56 PM
 */
public class Exporter {

    private static final Log LOG = LogFactory.getLog(Exporter.class);

    private static final String[] MYSQL_RESERVED_WORDS = {"ACCESSIBLE", "ALGORITHM", "ANALYZE", "ASENSITIVE", "BEFORE", "BIGINT", "BINARY", "BLOB", "CALL", "CHANGE", "CONDITION", "COPY", "DATA", "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DELAYED", "DESC", "DETERMINISTIC", "DIRECTORY", "DISCARD", "DISTINCTROW", "DIV", "DUAL", "EACH", "ELSEIF", "ENCLOSED", "ESCAPED", "EXCHANGE", "EXCLUSIVE", "EXIT", "EXPLAIN", "EXPORT", "FLOAT4", "FLOAT8", "FLUSH", "FORCE", "FULLTEXT", "GENERAL", "GROUP", "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IGNORE_SERVER_IDS", "IMPORT", "INFILE", "INOUT", "INPLACE", "INT1", "INT2", "INT3", "INT4", "INT8", "ITERATE", "KEYS", "KILL", "LEAVE", "LIMIT", "LINEAR", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY", "MASTER_HEARTBEAT_PERIOD", "MAXVALUE", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES", "NO_WRITE_TO_BINLOG", "OPTIMIZE", "OPTIONALLY", "OUT", "OUTFILE", "PARTITION", "PRIMARY", "PURGE", "RANGE", "READS", "READ_ONLY", "READ_WRITE", "REBUILD", "REGEXP", "RELEASE", "REMOVE", "RENAME", "REORGANIZE", "REPAIR", "REPEAT", "REPLACE", "REQUIRE", "RESIGNAL", "RETURN", "RLIKE", "SCHEMAS", "SECOND_MICROSECOND", "SENSITIVE", "SEPARATOR", "SHARED", "SHOW", "SIGNAL", "SLOW", "SPATIAL", "SPECIFIC", "SQLEXCEPTION", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STRAIGHT_JOIN", "TABLES", "TABLESPACE", "TERMINATED", "TINYBLOB", "TINYINT", "TINYTEXT", "TRIGGER", "UNDO", "UNLOCK", "UNSIGNED", "USE", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VARBINARY", "VARCHARACTER", "WHILE", "X509", "XOR", "YEAR_MONTH", "ZEROFILL"};

    private static String cleanName(String originalName) {

        String newName = originalName;
        if (Arrays.asList(MYSQL_RESERVED_WORDS).contains(newName.toUpperCase())) {
            newName = String.format("_%s", newName);
        }
        newName = remove(newName, "(");
        newName = remove(newName, ")");
        newName = remove(newName, ".");

        return newName;
    }


    public static org.apache.ddlutils.model.Database exportSchema(final String databaseName, final File mdbFile) throws IOException, SQLException {

        LOG.info(String.format("Exporting schema from '%s'", mdbFile.getAbsolutePath()));

        org.apache.ddlutils.model.Database database = new org.apache.ddlutils.model.Database();

        Database accessDb = Database.open(mdbFile, true);

        for (String tableName : accessDb.getTableNames()) {

            Table accessTable = accessDb.getTable(tableName);

            org.apache.ddlutils.model.Table table = new org.apache.ddlutils.model.Table();
            table.setName(cleanName(accessTable.getName()));

            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("TABLE: %s", table.getName()));
            }

            // Create all columns
            for (Column accessColumn : accessTable.getColumns()) {

                org.apache.ddlutils.model.Column column = new org.apache.ddlutils.model.Column();
                column.setName(cleanName(accessColumn.getName()));
                column.setTypeCode(accessColumn.getSQLType());
                column.setSizeAndScale(accessColumn.getLength(), accessColumn.getScale());

                if (LOG.isTraceEnabled()) {
                    LOG.trace(String.format("COLUMN: %s  type: %s, typeCode: %s, size: %s, scale: %s", column.getName(), column.getType(), column.getTypeCode(), column.getSize(), column.getScale()));
                }
                table.addColumn(column);
            }

            // Loop though the indexes (unique and non-unique)
            for (Index accessIndex : accessTable.getIndexes()) {

                org.apache.ddlutils.model.Index index = accessIndex.isUnique() ? new org.apache.ddlutils.model.UniqueIndex() : new org.apache.ddlutils.model.NonUniqueIndex();
                index.setName(cleanName(accessIndex.getName()));

                if (LOG.isTraceEnabled()) {
                    LOG.trace(String.format("INDEX: %s", index.getName()));
                }

                for (IndexData.ColumnDescriptor accessColumnDescriptor : accessIndex.getColumns()) {
                    Column accessColumn = accessColumnDescriptor.getColumn();
                    org.apache.ddlutils.model.Column column = table.findColumn(cleanName(accessColumn.getName()));
                    column.setPrimaryKey(accessIndex.isPrimaryKey());
                    org.apache.ddlutils.model.IndexColumn indexColumn = new org.apache.ddlutils.model.IndexColumn(column);
                    index.addColumn(indexColumn);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(String.format("INDEX COLUMN: %s  isPrimary: %s", indexColumn.getName(), column.isPrimaryKey()));
                    }

                }

                table.addIndex(index);
            }

            database.addTable(table);
        }
        accessDb.close();
        database.setName(databaseName);
        return database;
    }


    public static void exportDataToFile(final File mdbFile, final File exportFile) throws IOException, SQLException {

        org.apache.ddlutils.model.Database database = exportSchema(mdbFile.getName(), mdbFile);

        DataWriter dataWriter = new DataWriter(new FileOutputStream(exportFile));
        dataWriter.writeDocumentStart();
        Database accessDb = Database.open(mdbFile, true);

        List<DynaBean> beans = new ArrayList<DynaBean>();
        for (org.apache.ddlutils.model.Table dbTable : database.getTables()) {

            Table table = accessDb.getTable(dbTable.getName());

            List<Column> columns = table.getColumns();

            for (Map<String, Object> row : table) {

                DynaBean record = database.createDynaBeanFor(dbTable.getName(), false);


                for (Column col : columns) {
                    Object val = row.get(col.getName());
                    if (val != null) {
                        String columnName = cleanName(col.getName());
                        if (val instanceof Date) {
                            LOG.debug("Converting date to Timestamp");
                            long millis = ((Date) val).getTime();
                            Timestamp timestamp = new Timestamp(millis);
                            record.set(columnName, timestamp);
                        } else {
                            record.set(columnName, val);
                        }
                    }
                } // end iterating through columns

                beans.add(record);

            } // end iterating through rows
        } // end iterating through table names
        dataWriter.write(beans);
        dataWriter.writeDocumentEnd();
    }

    public static void importDataFile(final File dataFile, final File schemaFile, final org.apache.commons.dbcp.BasicDataSource dataSource) {

        DdlToDatabaseTask dbTask = new DdlToDatabaseTask();

        dbTask.addConfiguredDatabase(dataSource);
        dbTask.setSchemaFile(schemaFile);

        WriteDataToDatabaseCommand writeData = new WriteDataToDatabaseCommand();
        writeData.setEnsureForeignKeyOrder(true);
        writeData.setDataFile(dataFile);
        dbTask.addWriteDataToDatabase(writeData);
        dbTask.execute();

    }

    /**
     * Read the schema and data from the given Access database file and create the corresponding schema and data
     * in the passed in data source.  The data source can be from any database supported by the Apache DdlUtils project.
     *
     * @param dataSource
     * @param mdbFile
     * @throws IOException
     * @throws SQLException
     */
    public static void createDatabaseFromAccessFile(final DataSource dataSource, final File mdbFile) throws IOException, SQLException {

        Platform platform = PlatformFactory.createNewPlatformInstance(dataSource);
        org.apache.ddlutils.model.Database database = exportSchema(mdbFile.getName(), mdbFile);
        platform.createTables(database, false, false);

        Database accessDb = Database.open(mdbFile, true);

        List<DynaBean> records = new ArrayList<DynaBean>();
        for (org.apache.ddlutils.model.Table dbTable : database.getTables()) {

            Table table = accessDb.getTable(dbTable.getName());

            List<Column> columns = table.getColumns();

            for (Map<String, Object> row : table) {

                DynaBean record = database.createDynaBeanFor(dbTable.getName(), false);

                for (Column col : columns) {
                    Object val = row.get(col.getName());
                    if (val != null) {
                        String columnName = cleanName(col.getName());
                        record.set(columnName, val);
                    }
                } // end iterating through columns

                records.add(record);
            } // end iterating through rows
        } // end iterating through table names

        platform.insert(database, records);
    }
}
