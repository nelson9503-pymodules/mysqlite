
class TB:

    def __init__(self, tbName: str, dbObj: object):
        self.tbName = tbName
        self.db = dbObj
        self.__getKeyCol()

    def drop(self):
        """
        Table drop itself from database.
        """
        self.db.cur.execute("DROP TABLE `{}`".format(self.tbName))

    def rename(self, newName: str):
        """
        Rename table.
        """
        self.db.cur.execute(
            "ALTER TABLE `{}` RENAME `{}`;".format(self.tbName, newName))
        self.tbName = newName

    def listCol(self) -> dict:
        """
        List out all columns in table.
        """
        self.db.cur.execute("PRAGMA table_info(`{}`)".format(self.tbName))
        values = self.db.cur.fetchall()
        cols = {}
        for col in values:
            cols[col[1]] = col[2]
            if col[3] == 1:
                cols[col[1]] += " NOT NULL"
            if col[5] == 1:
                cols[col[1]] += " PRIMARY KEY"
        return cols

    def countCol(self) -> int:
        """
        Count columns in table.
        """
        self.db.cur.execute("PRAGMA table_info(`{}`)".format(self.tbName))
        values = self.db.cur.fetchall()
        return len(values)

    def addCol(self, colName: str, colType: str):
        """
        Add a new column to table.
        """
        self.db.cur.execute("ALTER TABLE `{}` ADD `{}` {};".format(
            self.tbName, colName, colType))

    def dropCol(self, colName: str):
        """
        Drop a column from table.
        """
        if colName == self.keyCol:
            raise KeyError("Cannot delete key column.")
        data = self.query()
        cols = self.listCol()
        for key in data:
            data[key].pop(colName)
        self.drop()
        self.db.commit()
        self.db.createTB(self.tbName, self.keyCol,
                         cols[self.keyCol].replace("NOT NULL PRIMARY KEY", ""))
        for col in cols:
            if col == self.keyCol:
                continue
            if col == colName:
                continue
            self.addCol(col, cols[col])
        self.update(data)

    def countData(self) -> int:
        self.db.cur.execute("SELECT COUNT(*) FROM `{}`;".format(self.tbName))
        values = self.db.cur.fetchall()
        return values[0][0]

    def dropData(self, keyVal: any):
        keyVal = self.__valueToString(keyVal)
        self.db.cur.execute("DELETE FROM `{}` WHERE `{}` = {};".format(
            self.tbName, self.keyCol, keyVal))

    def query(self, column: str = "*", condition="") -> dict:
        """
        Query table in dictionary.
        """
        sql = "SELECT {} FROM `{}`".format(column, self.tbName)
        if not condition == "":
            sql += " " + condition
        self.db.cur.execute(sql)
        values = self.db.cur.fetchall()
        cols = list(self.listCol().keys())
        results = {}
        for value in values:
            results[value[0]] = {}
            for i in range(1, len(value)):
                results[value[0]][cols[i]] = value[i]
        return results

    def update(self, data: dict):
        """
        Insert for data with new keys;
        Update for data with exist keys.
        """
        if len(data) == 0:
            return
        # part 1
        cols = list(self.listCol().keys())
        sqlPart1 = "("
        for col in cols:
            sqlPart1 += "`" + col + "`,"
        if sqlPart1[-1] == ",":
            sqlPart1 = sqlPart1[:-1]
        sqlPart1 += ")"
        # part 2
        sqlPart2 = ""
        for keyVal in data:
            vals = data[keyVal]
            vals[self.keyCol] = keyVal
            sqlPart2 += "("
            for col in cols:
                if not col in vals or vals[col] == None:
                    val = "null"
                else:
                    val = self.__valueToString(vals[col])
                sqlPart2 += val + ","
            if sqlPart2[-1] == ",":
                sqlPart2 = sqlPart2[:-1]
            sqlPart2 += "),"
        if sqlPart2[-1] == ",":
            sqlPart2 = sqlPart2[:-1]
        # part 3
        sqlPart3 = " ON CONFLICT(`{}`) ".format(self.keyCol)
        sqlPart3 += "DO UPDATE SET "
        for col in cols:
            if col == self.keyCol:
                continue
            sqlPart3 += "`{col}` = `{col}`,".format(col=col)
        if sqlPart3[-1] == ",":
            sqlPart3 = sqlPart3[:-1]
        # group parts
        sql = "INSERT INTO `{}`".format(self.tbName)
        sql += sqlPart1 + " VALUES "
        sql += sqlPart2
        sql += sqlPart3 + ";"
        self.db.cur.execute(sql)

    def __getKeyCol(self):
        cols = self.listCol()
        for col in cols:
            if "PRIMARY KEY" in cols[col]:
                self.keyCol = col
                return
        raise KeyError("Table has no key column.")

    def __valueToString(self, val: any) -> str:
        if type(val) == str:
            val = '"' + val + '"'
        else:
            val = str(val)
        return val
