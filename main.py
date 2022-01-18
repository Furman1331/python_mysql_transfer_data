from cmath import exp
import os
import asyncio
import mysql.connector
from mysql.connector import errorcode
from progress.bar import Bar
from progress.spinner import Spinner
from pick import pick
from dotenv import load_dotenv
load_dotenv()

config = {
    'user': os.environ.get('database_user'),
    'password': os.environ.get('database_password'),
    'host': os.environ.get('database_host'),
    'database': os.environ.get('database_name')
}

avaiableNewSettingTables = ['settings_shortcuts', 'settings_view_for_employee', 'settings_view_leaves', 'settings_view_overhours']
def main():
    print("BEFORE USE - Don't forget you need to create settings child tables but DON'T DELETE FIELD FROM settings YET !")
    print("The same situation on event_history table. you need to create it but DON'T DROP COLUMNS FROM TABLE event!!")
    understand = askUserBool("Understand ? (y/n) [y]:")
    if(understand):
        action = askUserForAction()

        if(action == 0): # Transfer from settings to child.
            try:
                connection = mysql.connector.connect(**config)
                if(connection.is_connected()):
                    print("Connect to database - correct.")
                    settingsColumnName = getNameOfColumns(connection, 'settings')

                    tablesValidation = {}
                    tableOfColumns = {}
                    settingChilds = multiSelectOption('Pick settings you want to transfer, press SPACE to select', avaiableNewSettingTables)
                    for tableName in settingChilds:
                        tablesValidation[tableName] = True
                        settingColumns = getNameOfColumns(connection, tableName)
                        if(len(settingColumns) != 0 ):
                            for settingColumn in settingColumns:
                                if not settingColumn in settingsColumnName:
                                    tablesValidation[tableName] = False

                            if(tablesValidation[tableName] == True):
                                tableOfColumns[tableName] = settingColumns
                        else:
                            print("[ERROR] Table "+tableName+" does not exist - rejected!")
                            tablesValidation[tableName] = False

                    for tableName in tablesValidation:
                        if(tablesValidation[tableName] == True):
                            isDataTransfered = transferDataFromTableToTable(connection, tableName, tableOfColumns[tableName], 'settings')
                            if(isDataTransfered):
                                print("[DONE] TABLE "+tableName+" imported correctly !")
                        else:
                            print("[ERROR] Some field from "+tableName+" not in table settings - rejected")
                else:
                    print("Connecting to database falied.")
            except mysql.connector.Error as error:
                if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Cannot access to database, maybe username or password is wrong.")
                elif error.errno == errorcode.ER_BAD_DB_ERROR:
                    print("Chosen database does not exist.")
                else:
                    print(error)
            else:
                connection.close()
        elif(action == 1):
            try:
                connection = mysql.connector.connect(**config)
                if(connection.is_connected()):
                    print("Connect to database - correct.")
                    
                    isEventsValid = True
                    eventsColumnName = getNameOfColumns(connection, 'event')
                    eventsColumnName[eventsColumnName.index('id')] = 'event_id'
                    eventHistryColumnName = getNameOfColumns(connection, 'event_history')
                    eventHistryColumnName.remove("id")
                    if(len(eventHistryColumnName) != 0 ):
                        for eventHistoryName in eventHistryColumnName:
                            if eventHistoryName != 'event_id' and not eventHistoryName in eventsColumnName:
                                isEventsValid = False
                    else:
                        print("[ERROR] Table event_histry does not exist - rejected!")
                        isEventsValid = False

                    if isEventsValid:
                        isDataTransfered = transferDataFromTableToTable(connection, 'event_history', eventHistryColumnName, 'event')
                        if(isDataTransfered):
                            print("[DONE] TABLE event_history imported correctly !")
                    else:
                        print("[ERROR] Some field from event_history not in table events - rejected")
            except mysql.connector.Error as error:
                if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Cannot access to database, maybe username or password is wrong.")
                elif error.errno == errorcode.ER_BAD_DB_ERROR:
                    print("Chosen database does not exist.")
                else:
                    print(error)
            else:
                connection.close()
    else:
        print("Script rejected.")

def getNameOfColumns(connection, table_name):
    arrayOfColumns = []
    current = connection.cursor()

    current.execute("SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA` = '"+os.environ.get('database_name')+"' AND `TABLE_NAME` = '"+table_name+"';")
    for name in current.fetchall():
        arrayOfColumns.append(name[0]) # Getting name of settings column.

    return arrayOfColumns

def transferDataFromTableToTable(connection, table, columns, parent):
    if not checkIsTableBlank(connection, table):
        question = askUserBool("Table "+table+" isn't empty, do you want to clean all data and import again ? (y/n) [y]:")
        if(question):
            emptyTable(connection, table)
            return transferDataFromTableToTable(connection, table, columns, parent)
        else:
            print("Skipping to next table.")
            return False
    else:
        print("Initialize Query...")
        selectString = ''
        lastElement = columns[-1]
        for column in columns:
            if(column != 'id'):
                if(column == lastElement):
                    selectString = selectString+' `'+(parent == 'event' and (column == 'event_id' and "id" or column) or column)+'`'
                else:
                    selectString = selectString+' `'+(parent == 'event' and (column == 'event_id' and "id" or column) or column)+'`,'

        try:
            bar = Bar('Progress...', max=countFromTable(connection, 'event'))
            query = connection.cursor()
            query.execute("SELECT "+selectString+" FROM `"+parent+"`;")
            if parent == 'event':
                for values in query.fetchall():
                    bar.next()
                    value = ','.join([str(data if data else "null") for data in values])
                    query.execute("INSERT INTO `"+table+"` ("+selectString[:-4]+"`event_id`) VALUES ("+value+")")
            else:
                insertString = ''
                for values in query.fetchall():
                    value = ','.join([str(data if data else "null") for data in values])
                    insertString = insertString+'('+value+'), '

                query.execute("INSERT INTO `"+table+"` ("+selectString+") VALUES "+insertString[:-2])

            connection.commit()
            query.close()
            return True
        except mysql.connector.Error as err:
            print("[ERROR] Something went wrong: {}".format(err))

def dropColumnsFromSettings(connection, columnsNames):
    query = connection.cursor()
    queryString = ''
    lastElement = columnsNames[-1]
    for column in columnsNames:
        if(column != 'id'):
            if(column == lastElement):
                queryString = queryString+'DROP COLUMN `'+column+'`;'
            else:
                queryString = queryString+'DROP COLUMN `'+column+'`, '

    try:
        print("ALTER TABLE `settings` "+queryString)
        query.execute("ALTER TABLE `settings` DROP COLUMN"+queryString)
        print("[DONE] Dropped columns from settings - complete")
        query.close()
    except mysql.connector.Error as err:
        print("[ERROR] Something went wrong: {}".format(err))

def checkIsTableBlank(connection, table):
    try:
        query = connection.cursor()
        query.execute("SELECT * FROM `"+table+"`")
        countResults = len(query.fetchall())
        query.close()
        if countResults == 0:
            return True
        else:
            return False
    except mysql.connector.Error as err:
        print("[ERROR] Something went wrong: {}".format(err))

def emptyTable(connection, table):
    try:
        query = connection.cursor()
        query.execute("DELETE FROM `"+table+"`")
        connection.commit()
        query.close()
    except mysql.connector.Error as err:
        print("[ERROR] Something went wrong: {}".format(err))

def askUserBool(msg):
    while True:
        try:
            passing = str(input(msg))
            print(passing)
            if passing == 'y' or passing == '':
                return True
            elif passing == 'n':
                return False
            else:
                print("This command didn't match.")
                continue
        except ValueError:
            print("This command didn't match.")
            continue

def askUserForAction():
    title = 'Please choice what you want to do:'
    options = ['Transfer data from table settings to child tables', 'Transfer data about event histry']
    option, index = pick(options, title, indicator="=>")
    return index

def multiSelectOption(title, options):
    selected = pick(options, title, multiselect=True, indicator="=>", min_selection_count=1)
    tableNames = []
    for select in selected:
        tableNames.append(select[0])
    return tableNames

def countFromTable(connection, table):
    try:
        query = connection.cursor(buffered=True)
        query.execute("SELECT COUNT(`id`) FROM `"+table+"`;")
        countResults = query.fetchone()[0]
        return countResults
        query.close()
    except mysql.connector.Error as err:
        print("[ERROR] Something went wrong: {}".format(err))

if(__name__) == '__main__':
    try:
        main()
    except MemoryError:
        sys.stderr.write("Maximum Memory Exceeded")
        sys.exit(-1)