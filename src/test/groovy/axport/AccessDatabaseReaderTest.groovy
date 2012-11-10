/**
 * 
 */
package axport

import org.apache.ddlutils.io.DatabaseIO
import org.apache.ddlutils.model.Database

import axport.AccessDatabaseReader;

import groovy.util.GroovyTestCase

/**
 * @author kris
 *
 */
class AccessDatabaseReaderTest extends GroovyTestCase {

	/**
	 * Test method for {@link com.guslabs.axport.AccessDatabaseReader#read()}.
	 */
	void testRead() {
		File file = new File("src/test/resources/RSTA_2012_League.mdb")
		Database database = new AccessDatabaseReader(srcdatabase: file).read()
		new DatabaseIO().write(database, "src/test/resources/access-schema.xml");
	}
}
