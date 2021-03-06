import sqlite3
from .obj_table import TB

# There are two main objects: Database and Table.
# Users connect to database via Database object,
# and then get the Table object to control data.


class DB:

    def __init__(self, db_path: str):
        """
        Connect to database.
        If not database exists, create a new one.
        """
        self.conn = sqlite3.connect(
            db_path, timeout=86400)  # set 24hrs timeout
        self.cur = self.conn.cursor()
        self.db_path = db_path

    def commit(self):
        """
        Commit changes on database.
        Any changes via this package would not affect the db file 
            until user commit the chanages.
        """
        self.conn.commit()

    def close(self):
        """
        Close connection from database.
        """
        self.conn.close()

    def list_tb(self) -> list:
        """
        List out all Tables in database.
        """
        sql = "SELECT name from sqlite_master WHERE type='table';"
        values = self.execute(sql)
        # values like this:
        #   [[tb,],[tb,],[tb,],]
        tbs = []
        for tb in values:
            tbs.append(tb[0])
        return tbs

    def add_tb(self, tb_name: str, key_col_name: str, key_data_type: str) -> object:
        """
        Create a new table with a key column.
        Users should define the key column name and its data type(e.g. CHAR(10)).
        To expand the table, please use Table.add_col to increase number of columns.
        This method will return the Table object for created table.
        """
        sql = "CREATE TABLE `{}` (`{}` {} NOT NULL PRIMARY KEY);".format(
            tb_name, key_col_name, key_data_type)
        self.execute(sql)
        tb = self.TB(tb_name)  # Create TB object
        return tb

    def TB(self, tb_name: str) -> object:
        """
        Obtain table object by using table name.
        If user try to control a table not exists, error will be rasied.
        (You should create the table before you control it.)
        """
        tb = TB(tb_name, self)
        return tb

    def execute(self, sql_quote: str) -> list:
        """
        execute sql quote directly.
        return a 2-d list: [[val, val], [val, val], ...]
        """
        values = self.cur.execute(sql_quote)
        values = values.fetchall()
        return values
