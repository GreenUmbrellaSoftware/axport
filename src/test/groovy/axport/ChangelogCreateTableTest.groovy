package axport;

import static org.junit.Assert.*;
import axport.ChangelogCreateTable;
import axport.Column;
import axport.ForeignKey;
import axport.Index;
import axport.PrimaryKey;
import axport.Table;
import axport.UniqueKey;

class ChangelogCreateTableTest extends GroovyTestCase {
	
	void testCreate() {
		def writer = new FileWriter('sample3.xml')
		
		def tables =[]
		def dept = new Table(tableName:'departments')
		dept.columns << new Column(name:'id', type:'number(4,0)', nullable:false)
		dept.columns << new Column(name:'dname', type:'varchar2(14)', remarks:'Department name')
		dept.primaryKey = new PrimaryKey(tableName:'departments', constraintName:'dept_pk', columnNames:'id')
		dept.uniqueKeys << new UniqueKey(tableName:'departments', constraintName:'dept_uk', columnNames:'dname')
		tables << dept
		
		def emp = new Table(tableName:'employees', remarks:'All employees known in HR system')
		emp.columns << new Column(name:'id', type:'number(4,0)', nullable:'false')
		emp.columns << new Column(name:'ename', type:'varchar2(14)', remarks:'Full name')
		emp.columns << new Column(name:'dept_id', type:'number(4,0)', nullable:'false')
		emp.primaryKey = new PrimaryKey(tableName:'employees', constraintName:'emp_pk', columnNames:'id')
		emp.foreignKeys << new ForeignKey( baseTableName:'employees', constraintName:'emp_dept_fk', baseColumnNames:'dept_id',
										   referencedTableName:'dept', referencedColumnNames:'id')
		def ind1 = new Index(tableName:'employees', indexName:'emp_ind1')
		ind1.columns << new Column(name:'dept_id')
		emp.indexes << ind1
		tables << emp
		
		def cct = new ChangelogCreateTable(author: "james")
		cct.generate(writer, tables)
	}

}
