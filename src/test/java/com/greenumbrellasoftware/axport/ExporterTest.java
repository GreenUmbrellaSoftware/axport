package com.greenumbrellasoftware.axport;

import static junit.framework.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.junit.Test;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: Kris
 * Date: 3/16/13
 * Time: 2:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class ExporterTest {

    private static final Log LOG = LogFactory.getLog(ExporterTest.class);

    @Test
    public void testExportSchema() throws Exception {
        Database database = Exporter.exportSchema(new File("src/test/resources/RSTA_2012_League.mdb"));
        File schemaFile = File.createTempFile(String.format("%s-", ExporterTest.class.getSimpleName()), ".xml");
        LOG.debug(String.format("Writing schema file to %s", schemaFile.getAbsolutePath()));
        assertFalse(schemaFile.length() > 0);
        new DatabaseIO().write(database, schemaFile.getAbsolutePath());
        assertTrue(schemaFile.length() > 0);
    }
}
