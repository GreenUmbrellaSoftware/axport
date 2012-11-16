/**
 * 
 */
package axport

import static org.junit.Assert.*
import groovy.sql.Sql;
import groovy.util.logging.Log;

import org.junit.Test
import org.junit.BeforeClass



/**
 * @author Kris
 *
 */
@Log
class DerbyDdlToDatabaseTest {

	static final String derbyUrl = "jdbc:derby:src/test/resources/derby/petclinic;create=true"
	static final String derbyDriverClassName = 'org.apache.derby.jdbc.EmbeddedDriver'
	static final String derbyUsername = "sa"
	static final String derbyPassword = ""
	static final String petclinicSchemaFileName = 'src/test/resources/petclinic/schema.xml'
	static final String petclinicDataFileName = 'src/test/resources/petclinic/data.xml'

	static final AntBuilder ant = new AntBuilder()
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	static void beforeClass() {

		assertNotNull (ant)
		ant.taskdef (
				name: 'ddlToDatabase',
				classname: 'org.apache.ddlutils.task.DdlToDatabaseTask'
				)

		ant.ddlToDatabase  {
			database (
					url: derbyUrl,
					driverClassName: derbyDriverClassName,
					username: derbyUsername,
					password: derbyPassword
					)
			fileset (file: petclinicSchemaFileName)
			createDatabase (failonerror: true )
			//			writeSchemaToDatabase
			writeDataToDatabase  (datafile: petclinicDataFileName)
		}
	}

	@Test
	void testSchemaFileExists() {
		Sql sql = Sql.newInstance( derbyUrl, derbyUsername,
				derbyPassword, derbyDriverClassName )
		assertNotNull (sql)
		sql.eachRow( 'select * from PERSON' ) { log.info "$it.ID -- ${it.LAST_NAME} --" }
		assertTrue (true)
	}

}
