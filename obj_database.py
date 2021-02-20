import sqlite3
from .obj_table import TB


class DB:

    def __init__(self, dbPath: str):
        """
        Connect to database.
        If not database exists, create a new one.
        """
        self.conn = sqlite3.connect(dbPath, timeout=86400)
        self.cur = self.conn.cursor()
        self.dbPath = dbPath

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

    def count_tb(self) -> int:
        """
        Count the tables in database.
        """
        self.cur.execute("SELECT name from sqlite_master WHERE type='table';")
        values = self.cur.fetchall()
        return len(values)

    def create_tb(self, tbName: str, keyName: str, keyType: str) -> object:
        """
        Create a new table with a key column.
        """
        self.cur.execute("CREATE TABLE `{}` (`{}` {} NOT NULL PRIMARY KEY);".format(
            tbName, keyName, keyType))
        tb = self.TB(tbName)
        return tb

    def TB(self, tbName: str) -> object:
        """
        Connect to a table.
        """
        tb = TB(tbName, self)
        return tb

    def execute(self, quote: str) -> list:
        """
        execute sql quote directly.
        """
        values = self.cur.execute(quote)
        return values
