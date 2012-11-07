package axport
import com.healthmarketscience.jackcess.Column
import com.healthmarketscience.jackcess.Database
import com.healthmarketscience.jackcess.Index
import com.healthmarketscience.jackcess.IndexData.ColumnDescriptor;
import com.healthmarketscience.jackcess.Table

import groovy.util.logging.Log

@Log
class AccessDatabaseReader {


	Database accessDb
	org.apache.ddlutils.model.Database database =  new org.apache.ddlutils.model.Database()

	private AccessDatabaseReader() {
		// intentionally blank
	}

	private AccessDatabaseReader(File file) {
		accessDb = Database.open(file, true)
		database.name = file.name
	}

	static org.apache.ddlutils.model.Database read (File file) {
		AccessDatabaseReader adr = new AccessDatabaseReader(file)
		return adr.readDatabase()
	}

	private org.apache.ddlutils.model.Database readDatabase () {
		readTables()
		return database
	}

	private void readTables() {
		
		log.info accessDb.databasePassword

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
				log.info(accessColumn.complexInfo)
				table.addColumn(column)
			}
			
			database.addTable(table);
		}
	}


	private void logTableInfo(final Table table) {



		log.info """
=========================
Table ${table.name}
   rowCount ${table.rowCount}
   columnCount ${table.columnCount}
"""
		for (Column column : table.columns ) {
			logColumnInfo (column)
		}
	}

	private void logColumnInfo (final Column column) {

		log.info """
       Column ${column.name}
          type: ${column.type}
		  SQLType: ${column.SQLType}
          length: ${column.length}
		  precision: ${column.precision}
"""
	}
}
