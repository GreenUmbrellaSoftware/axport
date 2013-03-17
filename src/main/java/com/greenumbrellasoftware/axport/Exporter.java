package com.greenumbrellasoftware.axport;

import com.healthmarketscience.jackcess.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: Kris
 * Date: 3/16/13
 * Time: 1:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class Exporter {

    private static final Log LOG = LogFactory.getLog(Exporter.class);

    public static org.apache.ddlutils.model.Database exportSchema(final File mdbFile) throws IOException, SQLException {

        LOG.info(String.format("Exporting schema from '%s'", mdbFile.getAbsolutePath()));

        org.apache.ddlutils.model.Database database = new org.apache.ddlutils.model.Database();

        Database accessDb = Database.open(mdbFile, true);

        for (String tableName : accessDb.getTableNames()) {

            Table accessTable = accessDb.getTable(tableName);

            org.apache.ddlutils.model.Table table = new org.apache.ddlutils.model.Table();
            table.setName(accessTable.getName());

            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("TABLE: %s", table.getName()));
            }

            // Create all columns
            for (Column accessColumn : accessTable.getColumns()) {

                org.apache.ddlutils.model.Column column = new org.apache.ddlutils.model.Column();
                column.setName(accessColumn.getName());
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
                index.setName(accessIndex.getName());

                if (LOG.isTraceEnabled()) {
                    LOG.trace(String.format("INDEX: %s", index.getName()));
                }

                for (IndexData.ColumnDescriptor accessColumnDescriptor : accessIndex.getColumns()) {
                    Column accessColumn = accessColumnDescriptor.getColumn();
                    org.apache.ddlutils.model.Column column = table.findColumn(accessColumn.getName());
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
            org.apache.ddlutils.model.Table table = database.findTable(accessTable.getName());

            for (Index accessIndex : accessTable.getIndexes()) {

                if (accessIndex.isForeignKey()) {

                    Index accessFk = accessIndex.getReferencedIndex();
                    org.apache.ddlutils.model.ForeignKey fk = new org.apache.ddlutils.model.ForeignKey();
                    fk.setName(accessFk.getName());
                    Table accessForeignTable = accessFk.getTable();
                    org.apache.ddlutils.model.Table foreignTable = database.findTable(accessForeignTable.getName());
                    fk.setForeignTable(foreignTable);

                    org.apache.ddlutils.model.Reference ref = new org.apache.ddlutils.model.Reference();

                    for (IndexData.ColumnDescriptor accessLocalCol : accessIndex.getColumns()) {
                        org.apache.ddlutils.model.Column localCol = table.findColumn(accessLocalCol.getName());
                        ref.setLocalColumn(localCol);
                    }

                    for (IndexData.ColumnDescriptor accessFkCol : accessFk.getColumns()) {
                        org.apache.ddlutils.model.Column refCol = foreignTable.findColumn(accessFkCol.getName());
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
}
