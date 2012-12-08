package axport

import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.dom4j.Document;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

class Main {

	private final static def USAGE = """
MS Access Database Export Utility (axport)

Options:

-srcdatabase <srcdatabase>            MS Access database file (defaults to 'data.mdb')
-destschemafile <destschemafile>      destination schema file (defaults to '[database name]-schema.xml')
-destdatafile <destdatafile>          destination data file (defaults to '[database name]-data.xml')

"""

	static main(args) {

		if ( args && "-help".equals(args[0])) {
			println USAGE
			System.exit(0)
		}

		AccessSchemaExporter schemaExporter = new AccessSchemaExporter()
		AccessDataExporter dataExporter = new AccessDataExporter()
		args.eachWithIndex { arg, idx ->

			int nextIdx = idx+1
			if ("-srcdatabase".equals(arg)) {
				if (nextIdx >= args.size()) {
					println USAGE
					System.exit(1)
				}
				else {
					File srcdatabase = new File (args[nextIdx])
					schemaExporter.srcdatabase = srcdatabase
					dataExporter.srcdatabase = srcdatabase
				}
			}
		}

		// Get the database structure
		Database database = schemaExporter.export()

		// Write the database schema to a file
		new DatabaseIO().write(database, "${database.name}-schema.xml")

		// Get Xml document representation of the data
		Document document = dataExporter.export()

		// Write the datbase data to a file
		FileOutputStream fos = new FileOutputStream("${database.name}-data.xml")
		OutputFormat format = OutputFormat.createPrettyPrint()
		XMLWriter writer = new XMLWriter(fos, format)
		writer.write(document)
		writer.flush()

	}
}
