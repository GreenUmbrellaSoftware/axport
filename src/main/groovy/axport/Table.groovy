package axport

class Table {
  Map attributes = [:]
  Map metaData = [:]
  List columns = []
  PrimaryKey primaryKey
  List uniqueKeys = []
  List foreignKeys = []
  List indexes = []
  Table(Map attribs) {
    attributes = attribs
  }
}
