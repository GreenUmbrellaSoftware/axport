package axport
import java.io.File;

import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.NonUniqueIndex;
import org.apache.ddlutils.model.UniqueIndex
import org.apache.ddlutils.model.Reference
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import com.healthmarketscience.jackcess.Column
import com.healthmarketscience.jackcess.Database
import com.healthmarketscience.jackcess.Index
import com.healthmarketscience.jackcess.IndexData.ColumnDescriptor;
import com.healthmarketscience.jackcess.Table
import groovy.xml.MarkupBuilder

import groovy.util.logging.Log

// TODO export dates in some format that will work upon import of data (e.g. 2002-03-12 00:00:00)
@Log
class AccessDataExporter {

	/**
	 * Source database that this reader will read.
	 * 
	 * <p>
	 * Defaults to "data.mdb"
	 * 
	 */
	File srcdatabase = new File ("data.mdb")


	Document export () {

		if (!srcdatabase.canRead()) {
			throw new IOException ("Cannot read database at ${srcdatabase?.absolutePath}." )
		}

		Database accessDb = Database.open(srcdatabase, true)
		return toXml (accessDb)
	}

	private Document toXml (Database db) {
		Document document = DocumentHelper.createDocument()
		Element root = document.addElement( "data" )
		for (String tableName : db.tableNames ) {

			Table table = db.getTable(tableName)
			List<Column> columns = table.columns

			for(Map<String, Object> row : table) {

				Element tableEl = root.addElement(tableName)
				for (Column col : columns) {
					Object val = row.get(col.name)
					if (val) {
						String colname = col.name
						//						colname = colname.replace("(", "")
						//						colname = colname.replace(")", "")
						tableEl.addAttribute(colname, val.toString())
					}
				} // end iterating through columns
			} // end iterating through rows
		} // end iterating through table names

		return document
	}
}