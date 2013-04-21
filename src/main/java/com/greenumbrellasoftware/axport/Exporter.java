package com.greenumbrellasoftware.axport;

import com.healthmarketscience.jackcess.*;

import static org.apache.commons.lang.StringUtils.remove;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Kris
 * Date: 3/16/13
 * Time: 1:56 PM
 * To change this template use File | Settings | File Templates.
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

    public static org.apache.ddlutils.model.Database exportSchema(final File mdbFile) throws IOException, SQLException {

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

        // Iterate through the tables a 2nd time in order to add foreign keys
        for (String tableName : accessDb.getTableNames()) {
            Table accessTable = accessDb.getTable(tableName);
            org.apache.ddlutils.model.Table table = database.findTable(cleanName(accessTable.getName()));

            for (Index accessIndex : accessTable.getIndexes()) {

                if (accessIndex.isForeignKey()) {

                    Index accessFk = accessIndex.getReferencedIndex();
                    org.apache.ddlutils.model.ForeignKey fk = new org.apache.ddlutils.model.ForeignKey();
                    fk.setName(cleanName(accessFk.getName()));
                    Table accessForeignTable = accessFk.getTable();
                    org.apache.ddlutils.model.Table foreignTable = database.findTable(cleanName(accessForeignTable.getName()));
                    fk.setForeignTable(foreignTable);

                    org.apache.ddlutils.model.Reference ref = new org.apache.ddlutils.model.Reference();

                    for (IndexData.ColumnDescriptor accessLocalCol : accessIndex.getColumns()) {
                        org.apache.ddlutils.model.Column localCol = table.findColumn(cleanName(accessLocalCol.getName()));
                        ref.setLocalColumn(localCol);
                    }

                    for (IndexData.ColumnDescriptor accessFkCol : accessFk.getColumns()) {
                        org.apache.ddlutils.model.Column refCol = foreignTable.findColumn(cleanName(accessFkCol.getName()));
                        ref.setForeignColumn(refCol);
                    }
                    fk.addReference(ref);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(String.format("FOREIGN KEY: %s  table: %s, foreign table: %s", fk.getName(), table.getName(), fk.getForeignTableName()));
                    }
                    table.addForeignKey(fk);
                }
            }

        }
        accessDb.close();
        return database;
    }

    public static Document exportDataToDocument(final File mdbFile) throws IOException {
        Database accessDb = Database.open(mdbFile, true);
        Document document = DocumentHelper.createDocument();
        Element root = document.addElement("data");
        for (String tableName : accessDb.getTableNames()) {

            Table table = accessDb.getTable(tableName);
            List<Column> columns = table.getColumns();

            for (Map<String, Object> row : table) {

                Element tableEl = root.addElement(cleanName(tableName));
                for (Column col : columns) {
                    Object val = row.get(col.getName());
                    if (val != null) {
                        String columnName = cleanName(col.getName());
                        tableEl.addAttribute(columnName, val.toString());
                    }
                } // end iterating through columns
            } // end iterating through rows
        } // end iterating through table names

        return document;
    }
}
