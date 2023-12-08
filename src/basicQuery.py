import sqlite3

# database connection/creation
database = sqlite3.connect('linkedIn.db')

# cursor
cursor = database.cursor()


def exposeTable(tableName: str, limit: int):
    # function performs a selection query on any table passed to it as an argument
    # displaying the limit number of rows

    fmt = "SELECT * FROM {} LIMIT {}"
    temp = cursor.execute(fmt.format(tableName, limit))
    result = temp.fetchall()
    [print(res) for res in result]


def showAllTables():
    # function displays all the tables in the database

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    result = cursor.fetchall()
    [print(table[0]) for table in result]
