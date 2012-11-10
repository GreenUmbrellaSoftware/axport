package axport
import java.io.File;

import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.NonUniqueIndex;
import org.apache.ddlutils.model.UniqueIndex
import org.apache.ddlutils.model.Reference

import com.healthmarketscience.jackcess.Column
import com.healthmarketscience.jackcess.Database
import com.healthmarketscience.jackcess.Index
import com.healthmarketscience.jackcess.IndexData.ColumnDescriptor;
import com.healthmarketscience.jackcess.Table

import groovy.util.logging.Log

@Log
class AccessDatabaseReader {

	File srcdatabase = new File ("data.mdb")


	org.apache.ddlutils.model.Database read () {

		if (!srcdatabase.canRead()) {
			throw new IOException ("Cannot read database at ${srcdatabase?.absolutePath}." )
		}
		Database accessDb = Database.open(srcdatabase, true)
		return toDatabaseModel(accessDb)
	}


	private org.apache.ddlutils.model.Database toDatabaseModel (Database accessDb) {
		org.apache.ddlutils.model.Database database =  new org.apache.ddlutils.model.Database()
		log.info accessDb.databasePassword

		database.name = accessDb.file.name - ".mdb"

		for (String tableName : accessDb.tableNames ) {

			org.apache.ddlutils.model.Table table = new org.apache.ddlutils.model.Table()
			Table accessTable = accessDb.getTable(tableName)
			table.name = accessTable.name

			for (Column accessColumn : accessTable.columns ) {

				org.apache.ddlutils.model.Column column = new org.apache.ddlutils.model.Column ()
				column.name = accessColumn.name
				column.autoIncrement = accessColumn.autoNumber
				column.typeCode = accessColumn.SQLType
				column.size = accessColumn.length

				table.addColumn(column)
			}

			// Loop through the NON-foreign indexes first
			for (Index accessIndex : accessTable.indexes) {
				if (!accessIndex.foreignKey) {
					org.apache.ddlutils.model.Index index  =   accessIndex.unique ? new UniqueIndex() : new NonUniqueIndex()
					index.name = accessIndex.name

					accessIndex.columns.each {

						org.apache.ddlutils.model.Column col = table.findColumn(it.name)
						if (accessIndex.primaryKey && 1 == accessIndex.columns.size()) {
							col.primaryKey = true
						}
						IndexColumn indexColumn = new IndexColumn (col)
						index.addColumn(indexColumn)
					}

					table.addIndex(index)
				}
			}
			database.addTable(table)
		}

		// 2nd loop through to add foreign keys
		for (String tableName : accessDb.tableNames ) {
			Table accessTable = accessDb.getTable(tableName)
			org.apache.ddlutils.model.Table table = database.findTable(accessTable.name)

			for (Index accessIndex : accessTable.indexes) {
				println "INDEX: $accessIndex"
				if (accessIndex.foreignKey) {

					Index accessFk = accessIndex.referencedIndex
					ForeignKey fk = new ForeignKey()
					fk.name = accessFk.name
					Table accessForeignTable = accessFk.table
					org.apache.ddlutils.model.Table foreignTable = database.findTable(accessForeignTable.name)
					fk.foreignTable = foreignTable

					Reference ref = new Reference()

					List<ColumnDescriptor> accessLocalCols = accessIndex.columns
					for (ColumnDescriptor accessLocalCol  : accessLocalCols) {
						println "localCol: $accessLocalCol"
						org.apache.ddlutils.model.Column localCol = table.findColumn(accessLocalCol.name)
						ref.localColumn = localCol
					}

					List<ColumnDescriptor> accessFkCols = accessFk.columns
					for (ColumnDescriptor accessFkCol : accessFkCols) {
						println "fkCol: $accessFkCol"

						org.apache.ddlutils.model.Column refCol = foreignTable.findColumn(accessFkCol.name)
						ref.foreignColumn = refCol

					}
					fk.addReference(ref)
					table.addForeignKey(fk)
				}
			}

		}

		return database
	}

}
