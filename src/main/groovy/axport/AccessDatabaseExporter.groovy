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
import groovy.xml.MarkupBuilder

import groovy.util.logging.Log

@Log
class AccessDatabaseExporter {

	/**
	 * Source database that this reader will read.
	 * 
	 * <p>
	 * Defaults to "data.mdb"
	 * 
	 */
	File srcdatabase = new File ("data.mdb")

	/**
	 * File that the data will be written to.
	 * 
	 * <p>
	 * Defaults to "data.xml"
	 * 
	 */
	File exportFile = new File ("mydata.xml")


	File export () {

		if (!srcdatabase.canRead()) {
			throw new IOException ("Cannot read database at ${srcdatabase?.absolutePath}." )
		}

		exportFile.delete()
		exportFile.createNewFile()

		Database accessDb = Database.open(srcdatabase, true)
		writeDataToFile(accessDb, exportFile)

		return exportFile
	}

	private writeDataToFile (Database db, File file) {

		StringBuilder createXml = new StringBuilder ('xml.data{\n')
		for (String tableName : db.tableNames ) {

			if ("WorkCategory".equals(tableName)) {

				Table table = db.getTable(tableName)
				List<Column> columns = table.columns

				for(Map<String, Object> row : table) {
					String record = "$tableName(" + columns.collect { col ->
						Object val = row.get(col.name)
						String result = ""
						switch (val) {

							case Date:
								result = "\"$val\""
								break

							case String:
								result = (String) val
							//							result = result.replace ("'", "\\'")
								result = result.replace ("\\", "/'")
								result = "\"$result\""
								break

							default:
								result = "$val"

						}
						String colname = col.name
						colname = colname.replace("(", "")
						colname = colname.replace(")", "")
						"$colname:$result"
					}.join(',') + ")\n"// end collecting columns
					createXml.append(record)
				} // end iterating through rows
			} // end if equals tableName
		} // end iterating through table names

		// Build the xml
		def writer = new StringWriter()
		def xml = new MarkupBuilder(writer)
		xml.doubleQuotes = true

		createXml.append('}')
		def shell = new GroovyShell(new Binding(xml: xml))

		String code = createXml.toString()

		File mycode = new File("mycode.groovy")
		mycode.text = code

		shell.evaluate (code)

		exportFile.append(writer.toString())

	}
}