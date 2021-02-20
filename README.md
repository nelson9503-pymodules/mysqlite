# mysqlite

Pack sqlite3 into object-oriented package. mysqlite provides miain READ, UPDATE, CREATE and DELETE methods so that users can control database without writting sql quotes.

## Quick Start

### Create a new database and table

```python
import mysqlite

# connect to database
db = mysqlite("maindb.db")

# create new table
tb = db.createTB("staff", "id", "INT")
tb.addCol("name", "CHAR(50)")
tb.addCol("age", "INT")

# update data
data = {
    0: {"name": "Tom", "age": 35},
    1: {"name": "Peter", "age": 40}
}
tb.update(data)

# commit change
db.commit()

# disconnect to database
db.close()
```

---

## Methods Discovery

**class |** DB ( dbPath: `str` )

* **func |** commit ( )
* **func |** close ( )
* **func |** list_tb ( ) **->** TableList: `list`
* **func |** count_tb ( ) **->** NumOfTable: `int`
* **func |** create_tb ( tbName: `str`, keyName: `str`, keyType: `str` ) **->** TBObject: `object`
* **func |** TB ( tbName: `str` ) **->** TBObject: `object`
* **func |** execute ( quote: `str` ) **->** values: `list`

**class |** TB # Users cannot access this class directly.

* **func |** drop ( )
* **func |** rename ( newName: `str`)
* **func |** list_col ( ) **->** listOfColumn: `dict`
* **func |** count_col ( ) **->** NumOfColumn: `int`
* **func |** add_col ( colName: `str`, colType: `str` )
* **func |** drop_col ( colName: `str` )
* **func |** count_data ( ) **->** NumOfData: `int`
* **func |** drop_data ( keyVal: `any` )
* **func |** query ( column: `str`, condition: `str` ) **->** results: `dict`
* **func |** update ( data: `dict` )

---
