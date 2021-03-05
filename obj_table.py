
# TB is Table object, which controls the data.

class TB:

    def __init__(self, tb_name: str, dbObj: object):
        self.tb_name = tb_name
        self.db = dbObj
        self.__get_key_col_name()

    def drop(self):
        """
        Table drop itself from database.
        """
        sql = "DROP TABLE `{}`".format(self.tb_name)
        self.db.execute(sql)

    def rename(self, new_name: str):
        """
        Rename table.
        """
        sql = "ALTER TABLE `{}` RENAME `{}`;".format(self.tb_name, new_name)
        self.db.execute(sql)
        self.tb_name = new_name

    def list_col(self) -> dict:
        """
        Get columns mapped with data type.
        """
        sql = "PRAGMA table_info(`{}`)".format(self.tb_name)
        values = self.db.execute(sql)
        cols = {}
        for col in values:
            cols[col[1]] = col[2]
            if col[3] == 1:  # column 3, 1 is True, means Not Null
                cols[col[1]] += " NOT NULL"
            if col[5] == 1:  # column 5, 1 is True, means Primary Key
                cols[col[1]] += " PRIMARY KEY"
        return cols

    def add_col(self, col_name: str, colType: str):
        """
        Add a new column to table.
        """
        sql = "ALTER TABLE `{}` ADD `{}` {};".format(
            self.tb_name, col_name, colType)
        self.db.execute(sql)

    def drop_col(self, col_name: str):
        """
        Drop a column from table.
        """
        if col_name == self.key_col_name:
            raise KeyError("Cannot delete key column.")
        # sqlite do not support delete column
        # we need copy the data and write it to new table
        # considering we don't drop column frequently,
        # this resources consuming is aceptable.
        data = self.query()
        cols = self.list_col()
        for key in data:
            data[key].pop(col_name)
        self.drop()
        self.db.commit()
        self.db.add_tb(self.tb_name, self.key_col_name,
                       cols[self.key_col_name].replace("NOT NULL PRIMARY KEY", ""))
        for col in cols:
            if col == self.key_col_name:  # added when create table
                continue
            if col == col_name:  # skip the column we want to delete
                continue
            self.add_col(col, cols[col])
        # the udpate function will ignore the column not exists
        self.update(data)

    def drop_data(self, key_val: any):
        """
        Drop entire row of data by using keys
        """
        key_val = self.__value_to_string(key_val)
        sql = "DELETE FROM `{}` WHERE `{}` = {};".format(
            self.tb_name, self.key_col_name, key_val)
        self.db.execute(sql)

    def query(self, column: str = "*", condition="") -> dict:
        """
        Query table in dictionary.
        """
        sql = "SELECT {} FROM `{}`".format(column, self.tb_name)
        if not condition == "":
            sql += " " + condition
        values = self.db.execute(sql)
        cols = list(self.list_col().keys())
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
        part1 = self.__generate_update_quote_part1(data)
        part2 = self.__generate_update_quote_part2(data)
        sql = "INSERT OR REPLACE INTO `{tb_name}` {part1} VALUES {part2};".format(
            tb_name=self.tb_name,
            part1=part1,
            part2=part2
        )
        self.db.execute(sql)

    # This function generate the update quote part 1.
    def __generate_update_quote_part1(self, data: dict) -> str:
        cols = list(self.list_col().keys())
        part1 = "("
        for col in cols:
            part1 += "`{}`,".format(col)
        if part1[-1] == ",":
            part1 = part1[:-1]
        part1 += ")"
        return part1

    # This function generate the update quote part 2.
    def __generate_update_quote_part2(self, data: dict) -> str:
        cols = list(self.list_col().keys())
        part2 = ""
        for key_val in data:
            vals = data[key_val] # get values except the key values
            vals[self.key_col_name] = key_val # add key value to it
            part2 += "("
            for col in cols:
                if not col in vals or vals[col] == None:
                    val = "null"
                else:
                    val = self.__value_to_string(vals[col])
                part2 += val + ","
            if part2[-1] == ",":
                part2 = part2[:-1]
            part2 += "),"
        if part2[-1] == ",":
            part2 = part2[:-1]
        return part2

    # This function get the key column name.
    def __get_key_col_name(self):
        cols = self.list_col()
        for col in cols:
            if "PRIMARY KEY" in cols[col]:
                self.key_col_name = col
                return
        raise KeyError("Table has no key column.")

    # Convert any values to string before write to sql quote.
    def __value_to_string(self, val: any) -> str:
        if type(val) == str:
            val = '"' + val + '"'
        else:
            val = str(val)
        return val
