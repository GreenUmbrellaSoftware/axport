package axport

import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;

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

		AccessDatabaseReader adr = new AccessDatabaseReader()
		args.eachWithIndex { arg, idx ->

			int nextIdx = idx+1
			if ("-srcdatabase".equals(arg)) {
				if (nextIdx >= args.size()) {
					println USAGE
					System.exit(1)
				}
				else {
					File srcdatabase = new File (args[nextIdx])
					adr.srcdatabase = srcdatabase
				}
			}
		}

		Database database = adr.read()
		new DatabaseIO().write(database, "${database.name}-schema.xml")
	}
}
