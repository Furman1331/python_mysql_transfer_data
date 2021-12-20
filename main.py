import os
from dns.rdataclass import NONE
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
load_dotenv()

config = {
    'user': os.environ.get('database_user'),
    'password': os.environ.get('database_password'),
    'host': os.environ.get('database_host'),
    'database': os.environ.get('database_name')
}

arrayOfNewSettingsTable = ['settings_shortcuts', 'settings_view_for_employee']
def main():
    try:
        connection = mysql.connector.connect(**config)

        for newTable in arrayOfNewSettingsTable:
            arrayOfResults = getNameOfColumns(connection, newTable)
            if(len(arrayOfResults) != 0):
                transferDataFromTableToTable(connection, newTable, arrayOfResults)
            else:
                print("Table "+ newTable +" does not exist. Create Table and try again.")
    except mysql.connector.Error as error:
        if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        elif error.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(error)
    else:
        connection.close()

def getNameOfColumns(connection, table_name):
    arrayOfColumns = []
    current = connection.cursor()

    current.execute("SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA` = 'rcp_dev' AND `TABLE_NAME` = '"+table_name+"';")
    for name in current.fetchall():
        arrayOfColumns.append(name[0]) # Getting name of settings column.

    return arrayOfColumns

def transferDataFromTableToTable(connection, table, columns):
    if not checkIsTableBlank(connection, table):
        question = askUserBool("Table "+ table +" isn't empty, do you want to clean all data and import again ? (y/n):")
        if question:
            emptyTable(connection, table)
            transferDataFromTableToTable(connection, table, columns)
        else:
            return
    else:
        selectString = ''
        lastElement = columns[-1]
        for column in columns:
            if(column != 'id'):
                if(column == lastElement):
                    selectString = selectString+' '+column
                else:
                    selectString = selectString+' '+column+','

        query = connection.cursor()
        query.execute("SELECT "+selectString+" FROM `settings`;")
        insertString = ''
        for values in query.fetchall():
            value = ','.join([str(data if data else "0") for data in values])
            insertString = insertString+'('+value+'), '
        query.execute("INSERT INTO `"+table+"` ("+selectString+") VALUES "+insertString[:-2])
        connection.commit()
        query.close()
        print("Import from settings to "+table+". Done correctly")

def emptyTable(connection, table):
    query = connection.cursor()
    query.execute("DELETE FROM `"+table+"`")
    connection.commit()
    query.close()

def checkIsTableBlank(connection, table):
    query = connection.cursor()
    query.execute("SELECT * FROM `"+table+"`")
    if len(query.fetchall()) == 0:
        return True
    else:
        return False

def askUserBool(msg):
    while True:
        try:
            passing = str(input(msg))
            if passing == 'y':
                return True
            elif passing == 'n':
                return False
            else:
                print("This command didn't match.")
                continue
        except ValueError:
            print("This command didn't match.")
            continue

if(__name__) == '__main__':
    try:
        main()
    except MemoryError:
        sys.stderr.write("Maximum Memory Exceeded")
        sys.exit(-1)