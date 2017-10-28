#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'ratings'
SECOND_TABLE_NAME = 'movies'
SORT_COLUMN_NAME_FIRST_TABLE = 'rating'
SORT_COLUMN_NAME_SECOND_TABLE = 'movieid1'
JOIN_COLUMN_NAME_FIRST_TABLE = 'movieid'
JOIN_COLUMN_NAME_SECOND_TABLE = 'movieid1'
##########################################################################################################
NO_OF_THREADS = 5

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    #pass #Remove this once you are done with implementation
    cur = openconnection.cursor()
    temp = "SELECT MIN(" + SortingColumnName + ") FROM " + InputTable + ";"
    cur.execute(temp)
    minval = cur.fetchone()[0]

    temp = "SELECT MAX(" + SortingColumnName + ") FROM " + InputTable + ";"
    cur.execute(temp)
    maxval = cur.fetchone()[0]

    interval = float (maxval-minval) / NO_OF_THREADS

    temp = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable + "\';"
    cur.execute(temp)
    InputTableSchema = cur.fetchall()

    for i in range(NO_OF_THREADS):
        tblName = "range_part" + str(i)
        temp = "CREATE TABLE " + tblName + " (" + InputTableSchema[0][0] + " " + InputTableSchema[0][1] + ");"
        cur.execute(temp)
        for each in range(1,len(InputTableSchema)):
            temp = "ALTER TABLE " + tblName + " ADD COLUMN " + InputTableSchema[each][0] + " " + InputTableSchema[each][1] + ";"
            cur.execute(temp)
    threads = [0,0,0,0,0]
    for i in range(NO_OF_THREADS):
        if i == 0:
            minvalue = minval
            maxvalue = minvalue + interval
        else:
            minvalue = maxvalue
            maxvalue = maxvalue + interval

        threads[i] = threading.Thread(target=Sorted,args=(InputTable,SortingColumnName,i,minvalue,maxvalue,openconnection))
        threads[i].start()

    for i in range(NO_OF_THREADS):
        threads[i].join()

    temp = "CREATE TABLE " + OutputTable + " (" + InputTableSchema[0][0] + " INTEGER);"
    cur.execute(temp)
    for each in range(1,len(InputTableSchema)):
            temp = "ALTER TABLE " + OutputTable + " ADD COLUMN " + InputTableSchema[each][0] + " " + InputTableSchema[each][1] + ";"
            cur.execute(temp)
    for i in range(NO_OF_THREADS):
        temp = "INSERT INTO " + OutputTable + " SELECT * FROM range_part" + str(i) + ";"
        cur.execute(temp)

    for i in range(NO_OF_THREADS):
        temp = "DROP TABLE IF EXISTS range_part" + str(i) + ";"
        cur.execute(temp)

    openconnection.commit()         

def Sorted(InputTable,SortingColumnName,i,minvalue,maxvalue,openconnection):
    cur = openconnection.cursor()
    tblName = "range_part" + str(i)
    if i == 0:
        temp = "INSERT INTO " + tblName + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " >= " + str(minvalue) + " AND " + SortingColumnName + " <= " + str(maxvalue) + " ORDER BY " + SortingColumnName + " ASC;"
    else:
        temp = "INSERT INTO " + tblName + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " > " + str(minvalue) + " AND " + SortingColumnName + " <= " + str(maxvalue) + " ORDER BY " + SortingColumnName + " ASC;"      
    cur.execute(temp)
    return

    
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    #pass # Remove this once you are done with implementation
    cur = openconnection.cursor()
    temp = "SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + ";"
    cur.execute(temp)
    mintbl1 = float(cur.fetchone()[0])

    temp = "SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + ";"
    cur.execute(temp)
    mintbl2 = float(cur.fetchone()[0])

    temp = "SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + ";"
    cur.execute(temp)
    maxtbl1 = float(cur.fetchone()[0])

    temp = "SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + ";"
    cur.execute(temp)
    maxtbl2 = float(cur.fetchone()[0])

    maxtbl = max(maxtbl1,maxtbl2)
    mintbl = min(mintbl1,mintbl2)
    interval = (maxtbl - mintbl) / NO_OF_THREADS

    temp = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable1 + "\';"
    cur.execute(temp)
    InputTableSchema1 = cur.fetchall()

    temp = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable2 + "\';"
    cur.execute(temp)
    InputTableSchema2 = cur.fetchall()

    temp = "CREATE TABLE " + OutputTable + " (" + InputTableSchema1[0][0] + " INTEGER);"
    cur.execute(temp)

    for each in range(1,len(InputTableSchema1)):
        temp = "ALTER TABLE " + OutputTable + " ADD COLUMN " + InputTableSchema1[each][0] + " " + InputTableSchema1[each][1] + ";"
        cur.execute(temp)

    for each in range(len(InputTableSchema2)):
        temp = "ALTER TABLE " + OutputTable + " ADD COLUMN " + InputTableSchema2[each][0] + " " + InputTableSchema2[each][1] + ";"
        cur.execute(temp)    

    for i in range(NO_OF_THREADS):
        tblName = "inputtable1_" + str(i)
        if i == 0:
            minvalue = mintbl
            maxvalue = minvalue + interval
            temp = "CREATE TABLE " + tblName + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " >= " + str(minvalue) + " AND " + Table1JoinColumn + " <= " + str(maxvalue)  + ";"
        else:
            minvalue = maxvalue
            maxvalue = minvalue + interval
            temp = "CREATE TABLE " + tblName + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " > " + str(minvalue) + " AND " + Table1JoinColumn + " <= " + str(maxvalue)  + ";"
        cur.execute(temp)
    
    for i in range(NO_OF_THREADS):
        tblName = "inputtable2_" + str(i)
        if i == 0:
            minvalue = mintbl
            maxvalue = minvalue + interval
            temp = "CREATE TABLE " + tblName + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " >= " + str(minvalue) + " AND " + Table2JoinColumn + " <= " + str(maxvalue) + ";"
        else:
            minvalue = maxvalue
            maxvalue = minvalue + interval
            temp = "CREATE TABLE " + tblName + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " > " + str(minvalue) + " AND " + Table2JoinColumn + " <= " + str(maxvalue) + ";"
        cur.execute(temp)

    for i in range(NO_OF_THREADS):
        OutputTableRange = "outtable_range" + str(i)
        temp = "CREATE TABLE " + OutputTableRange + " (" + InputTableSchema1[0][0] + " INTEGER);"
        cur.execute(temp)

        for each in range(1,len(InputTableSchema1)):
            temp = "ALTER TABLE " + OutputTableRange + " ADD COLUMN " + InputTableSchema1[each][0] + " " + InputTableSchema1[each][1] + ";"
            cur.execute(temp)

        for each in range(len(InputTableSchema2)):
            temp = "ALTER TABLE " + OutputTableRange + " ADD COLUMN " + InputTableSchema2[each][0] + " " + InputTableSchema2[each][1] + ";"
            cur.execute(temp)

    threads = [0,0,0,0,0]
    for i in range(NO_OF_THREADS):
        threads[i] = threading.Thread(target=Join,args=(Table1JoinColumn,Table2JoinColumn,openconnection,i))
        threads[i].start()

    for i in range(NO_OF_THREADS):
        threads[i].join()

    for i in range(NO_OF_THREADS):
        temp = "INSERT INTO " + OutputTable + " SELECT * FROM outtable_range" + str(i) + ";"
        cur.execute(temp)

    for i in range(NO_OF_THREADS):
        temp = "DROP TABLE IF EXISTS inputtable1_" + str(i) + ";" 
        temp1 = "DROP TABLE IF EXISTS inputtable2_" + str(i) + ";" 
        temp2 = "DROP TABLE IF EXISTS outtable_range" + str(i) + ";" 
        cur.execute(temp)   
        cur.execute(temp1) 
        cur.execute(temp2) 

    openconnection.commit()

def Join(Table1JoinColumn,Table2JoinColumn,openconnection,i):
    cur = openconnection.cursor()
    temp = """INSERT INTO outtable_range""" + str(i) + """ SELECT * FROM inputtable1_""" + str(i) + """ INNER JOIN inputtable2_""" + str(i) +""" ON inputtable1_""" + str(i) + """.""" + str(Table1JoinColumn).lower() + """ = inputtable2_""" + str(i) + """.""" + str(Table2JoinColumn).lower() + """;"""
    cur.execute(temp)
    return 


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment3
	print "Creating Database named as ddsassignment3"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
