# mysqlite

mysqlite packs sqlite3 into object-oriented package. mysqlite provides miain READ, UPDATE, CREATE and DELETE methods so that users can control database without writting sql quotes.

## Connect to Database

Users provide the path of database (.db) to connect the database. If the database not exists, database object will create it.

```python
import mysqlite

db = mysqlite.DB("path/of/database.db")
```

## Drop Database

Use drop method to drop the database itself.

```python
db.drop()
```

## Commit Changes

Any changes on the database and tables in it, the changes will not affect the database file until users commit them.

```python
db.commit()
```

## List Tables

Users can list out the tables in the database.

```python
tbs = db.list_tb()
```

## Create Table

To create a table, users must define the table name, key column name and the data type of keys. The new table conatins only the key column at first initialization. User should add the value columns to complete the table setting.

```python
tb = db.add_table("staff_table", "staff_id", "INT")
tb.add_col("name", "CHAR(100)")
tb.add_col("gender", "CHAR(1)")
```

## Get a Table

Users can get the table object via the database object.

```python
tb = db.TB("table_name")
```

## Drop Table

The method of dropping table is same as dropping database, the object will drops itself.

```python
tb.drop()
```

## Rename Table

To rename the table, users should provide the new name of table.

```python
tb.rename("new_table_name")
```

## List Column

This method returns the list of column names in table.

```python
cols = tb.list_col()
```

## Add and Drop Column

```python
tb.add_col("name", "CHAR(100)")
tb.drop_col("name")
```

## Drop Data

The only way to drop data is dropping the entire row of data by using key value.

```python
# we try to drop a staff information with staff id 100
tb.drop_data(100)
```

## Query Data

Query method return the query in dictionary ({keys: {col: value, col: value}, keys: {col: value, col: value}, ...}). Users can customize the query sql quote via the arguments `column` and `condition`.

```python
# we query the male staff names
result = tb.query(column="id, name", condition="WHERE gender = 'M'")

# The sql quote would be like this:
# SELECT id, name FROM staff_table WHERE gender = 'M';
```

## Update Data

The udpate method insert the data for new keys and udpate the data for existed keys. The data types of data should be consistent with the sql table.

```python

data = {
    100: {"name": "Nelson", "gender": "M"}, # 100, 200 are staff id
    200: {"name": "Mary", "gender": "F"}
}

tb.update(data)
```

## Execute SQL Quotes

Users can execute the sql quotes using this method.

```python
result = db.execute("sql_quote") # some quote not returning data will return None
```
