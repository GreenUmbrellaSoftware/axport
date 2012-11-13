/**
 * 
 */
package axport

import static org.junit.Assert.*

import org.junit.Test
import org.junit.BeforeClass



/**
 * @author Kris
 *
 */
class HsqlDatabaseToDdlTest {

	static final String hsqlUrl = "jdbc:hsqldb:src/test/resources/hsql/petclinic"
	static final String hsqlDriverClassName = 'org.hsqldb.jdbcDriver'
	static final String hsqlUsername = "sa"
	static final String hsqlPassword = ""
	static final String hsqlSchemaFileName = 'src/test/resources/hsql/petclinic-schema.xml'
	static final String hsqlDataFileName = 'src/test/resources/hsql/petclinic-data.xml'

	static final AntBuilder ant = new AntBuilder()
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	static void beforeClass() {

		ant.delete (file: 'hsqlSchemaFileName')
		ant.delete (file: 'hsqlDataFileName')
		ant.taskdef (
				name: 'databaseToDdl',
				classname: 'org.apache.ddlutils.task.DatabaseToDdlTask'
				)

		ant.databaseToDdl {
			database (
					url: hsqlUrl,
					driverClassName: hsqlDriverClassName,
					username: hsqlUsername,
					password: hsqlPassword
					)
			writeSchemaToFile (outputFile: hsqlSchemaFileName )
			writeDataToFile (outputFile: hsqlDataFileName)
		}
	}

	@Test
	void testSchemaFileExists() {
		assertTrue (new File(hsqlSchemaFileName).exists())
	}

	@Test
	void testDataFileExists() {
		assertTrue (new File(hsqlDataFileName).exists())
	}
}
