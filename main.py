import sqlite3
import configparser as cf
import pymssql

config = cf.ConfigParser()
config.read('config.ini')
serverName = config['SERVER_INFO']['SERVER_NAME']
dbName = config['SERVER_INFO']['DB_NAME']
id = config['SERVER_INFO']['ID']
pw = config['SERVER_INFO']['PW']

mssqlConn = pymssql.connect(serverName, id, pw, dbName)

print("연결할 DB 경로(.idx)")
db = input()

print("Query")
query = input()

print("생성 테이블 명칭")
tableName = input()

sqlite3Conn = sqlite3.connect(db)
sqlite3Cursor = sqlite3Conn.cursor()

try:
    sqlite3Cursor.execute(query)
except:
    print("invalid query!!")

checkQuery = "if exists (SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "')"
checkQuery += "drop table " + tableName
with mssqlConn.cursor() as mssqlCursor:
    try:
        mssqlCursor.execute(checkQuery)
        print("drop table")
    except:
        print("fail drop table query!!")

columnSize = 0
createTableQuery = "CREATE TABLE " + tableName + "("
res = sqlite3Cursor.fetchall()
for row in res:
    i = 0
    for k in row:
        createTableQuery += "col" + str(i)
        if isinstance(k, int):
            createTableQuery += " INT NULL DEFAULT (0), "
        elif isinstance(k, str):
            createTableQuery += " NVARCHAR(128) NULL DEFAULT (0), "
        i = i+1
    columnSize = i
    break

createTableQuery += ")"
print(createTableQuery)

with mssqlConn.cursor() as mssqlCursor:
    try:
        mssqlCursor.execute(createTableQuery)
    except:
        print("fail make table query!!")

for row in res:
    insertQuery = "INSERT INTO " + tableName + "("
    for i in range(columnSize):
        insertQuery += "col" + str(i) + ","
    insertQuery = insertQuery[:-1]
    insertQuery += ") VALUES ("
    for k in row:
        if isinstance(k, int):
            insertQuery += str(k) + ","
        elif isinstance(k, str):
            insertQuery += k + ","
    insertQuery = insertQuery[:-1]
    insertQuery += ")"
    with mssqlConn.cursor() as mssqlCursor:
        try:
            mssqlCursor.execute(insertQuery)
        except:
            print("fail insert data query!! : " + insertQuery)

sqlite3Conn.close()
mssqlConn.commit()
mssqlConn.close()

input("Press Enter to continue...")