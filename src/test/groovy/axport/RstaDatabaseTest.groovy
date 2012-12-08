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
class RstaDatabaseTest {

	static final String derbyUrl = "jdbc:derby:memory:rsta;create=true"
	static final String derbyDriverClassName = 'org.apache.derby.jdbc.EmbeddedDriver'
	static final String derbyUsername = "sa"
	static final String derbyPassword = "password"
	static final String petclinicSchemaFileName = 'RSTA_2012_League-schema.xml'
	static final String petclinicDataFileName = 'RSTA_2012_League-data.xml'
	static final String TEST_SQL = 'select * from Athlete'

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
		def athletes = []
		sql.eachRow( TEST_SQL ) {
			log.info "$it.Athlete -- ${it.Birth} --"
			athletes << it.toRowResult()
		}
		assertEquals (5, athletes.size())
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
