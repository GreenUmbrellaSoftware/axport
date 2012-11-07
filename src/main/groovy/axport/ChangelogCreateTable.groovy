package axport

class ChangelogCreateTable {
  String author
  def changesetId = 1
  def generate(writer, tables) {
    def eol = System.properties.'line.separator'
    writer << '<?xml version="1.0" encoding="UTF-8" standalone="no"?>' << eol

    def xml = new groovy.xml.MarkupBuilder(writer)
    xml.databaseChangeLog( xmlns : "http://www.liquibase.org/xml/ns/dbchangelog/1.9"
                         , "xmlns:xsi" : "http://www.w3.org/2001/XMLSchema-instance"
                         , "xsi:schemaLocation" : "http://www.liquibase.org/xml/ns/dbchangelog/1.9 http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-1.9.xsd"
                         ) {
      tables.each { table ->
        changeSet(author: author, id : changesetId++) {
          createTable( table.getAttributes() ) {
            table.columns.each { col ->
              column(col.getAttributes() ) {
                if(col.getConstraintsAttributes()) {
                  constraints(col.getConstraintsAttributes())
                }
              }
            }
          }
          if (table.primaryKey != null) {
            addPrimaryKey(table.primaryKey.getAttributes())
          }
          table.uniqueKeys.each { uk ->
            addUniqueConstraint(uk.getAttributes())
          }
          table.indexes.each{ index ->
            createIndex(index.getAttributes()) {
              index.columns.each { col ->
                column(col.getAttributes() )
              }
            }
          }
        }
      }
      
      // Create the foreign keys
      tables.each { table ->
        if (table.foreignKeys) {
          changeSet(author: author, id : changesetId++) {
            table.foreignKeys.each { fk ->
              addForeignKeyConstraint(fk.getAttributes())
            }
          }
        }
      }
    }
  }
}