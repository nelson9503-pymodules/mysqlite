import sqlite3
from .obj_table import TB


class DB:

    def __init__(self, db_path: str):
        """
        Connect to database.
        If not database exists, create a new one.
        """
        self.conn = sqlite3.connect(db_path, timeout=86400)
        self.cur = self.conn.cursor()
        self.db_path = db_path

    def commit(self):
        """
        Commit changes on database.
        """
        self.conn.commit()

    def close(self):
        """
        Close connection from database.
        """
        self.conn.close()

    def list_tb(self, filter: str = None) -> list:
        """
        List out all Tables in database.
        """
        sql = "SELECT name from sqlite_master WHERE type='table'"
        if not filter == None:
            sql += " AND name LIKE '%{}%'".format(filter)
        sql += ";"
        self.cur.execute(sql)
        values = self.cur.fetchall()
        tbs = []
        for tb in values:
            tbs.append(tb[0])
        return tbs

    def add_tb(self, tb_name: str, key_col_name: str, key_data_type: str) -> object:
        """
        Create a new table with a key column.
        """
        self.cur.execute("CREATE TABLE `{}` (`{}` {} NOT NULL PRIMARY KEY);".format(
            tb_name, key_col_name, key_data_type))
        tb = self.TB(tb_name)
        return tb

    def TB(self, tb_name: str) -> object:
        """
        Connect to a table.
        """
        tb = TB(tb_name, self)
        return tb

    def execute(self, sql_quote: str) -> list:
        """
        execute sql quote directly.
        """
        values = self.cur.execute(sql_quote)
        return values
