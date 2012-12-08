/**
 * 
 */
package axport

import static org.junit.Assert.*

import java.sql.SQLSyntaxErrorException;

import groovy.sql.Sql
import groovy.util.logging.Log

import org.junit.Test
import org.junit.Before
import org.junit.After



/**
 * @author Kris
 *
 */
@Log
class DerbyDdlToDatabaseTest {

	static final String derbyUrl = "jdbc:derby:memory:petclinic;create=true"
	static final String derbyDriverClassName = 'org.apache.derby.jdbc.EmbeddedDriver'
	static final String derbyUsername = "sa"
	static final String derbyPassword = "password"
	static final String petclinicSchemaFileName = 'src/test/resources/petclinic/schema.xml'
	static final String petclinicDataFileName = 'src/test/resources/petclinic/data.xml'
	static final String TEST_SQL = 'select * from PERSON'

	static final AntBuilder ant = new AntBuilder()
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	void before() {

		Sql sql = Sql.newInstance( derbyUrl, derbyUsername,
				derbyPassword, derbyDriverClassName )

		try {
			sql.eachRow( TEST_SQL ) { fail("database doesn't exist at this time so the sql should fail") }
		}
		catch (SQLSyntaxErrorException e) {
			assertNotNull (e)
		}
	}

	@After
	void after() {
		Sql sql = Sql.newInstance( derbyUrl, derbyUsername,
				derbyPassword, derbyDriverClassName )
		assertNotNull (sql)
		def people = []
		sql.eachRow( TEST_SQL ) {
			log.info "$it.ID -- ${it.LAST_NAME} --"
			people << it.toRowResult()
		}
		assertEquals (6, people.size())
	}

	@Test
	void testCreateDatabase() {
		createDatabase()
	}


	private void createDatabase() {
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
			writeSchemaToDatabase()
			writeDataToDatabase  (datafile: petclinicDataFileName)
		}
	}
}
