#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import os
import time
import math 

DATABASE_NAME = 'dds_assgn1'
ratingstablename = 'ratings'
INPUT_FILE_PATH = 'ratings.dat'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
   
   # Create Table with the three required fields and the other dummy fields
   start = time.time()
   command = """CREATE TABLE """ + ratingstablename + """ (userid INTEGER,d1 CHAR,movieid INTEGER,d2 CHAR,rating REAL,d3 CHAR,time BIGINT);"""
   cur = openconnection.cursor()
   cur.execute(command)
   openconnection.commit()

   # Insert Data into the created table
   command = """COPY """ + ratingstablename + """ (userid,d1,movieid,d2,rating,d3,time) FROM '""" + os.path.abspath(ratingsfilepath) + """' DELIMITER ':' CSV"""
   cur.execute(command)
   #cur.copy_from(open(ratingsfilepath),ratingstablename,sep=':')
   openconnection.commit()

   # Alter table to delete the dummy fields
   command = """ALTER TABLE """ + ratingstablename + """ DROP COLUMN d1,DROP COLUMN d2,DROP COLUMN d3,DROP COLUMN time;"""
   cur.execute(command)
   cur.close()
   openconnection.commit()

   # Print the time taken to create the table and insert data
   end = time.time()
   print "Ratings Table created and Data inserted"
   print "Time taken:",end-start

def rangepartition(ratingstablename, numberofpartitions, openconnection):

    # Pre-check the variables:
    if (numberofpartitions <=0 or math.floor(numberofpartitions) != math.ceil(numberofpartitions) ):
        print " No of partitions is not an integer"
        return
    # Creating the number of range partitions and printing them
    start = time.time()
    prefix = "range_part"
    partitions = []
    for i in range(0,numberofpartitions):
        temp = prefix + str(i)
        partitions.append(temp)
    print partitions

    # Increment in terms of the value and insert data with ratings in that section
    start_range = 0
    end_range = 0
    incr = float (5) / numberofpartitions
    for i in range(0,numberofpartitions):
        # Create the partition table
        command = """CREATE TABLE """ + str(partitions[i]) + """ (userid INTEGER,movieid INTEGER,rating REAL);"""
        cur = openconnection.cursor()
        cur.execute(command)
        openconnection.commit()

        end_range += incr
        #print start_range,end_range
        if (i==0):
            command = """INSERT INTO """ + str(partitions[i]) + """ (userid,movieid,rating) SELECT * FROM """ + ratingstablename + """ WHERE rating >= """ + str(start_range) + """ AND rating <= """ + str(end_range) + """;"""
        else:
            command = """INSERT INTO """ + str(partitions[i]) + """ (userid,movieid,rating) SELECT * FROM """ + ratingstablename + """ WHERE rating >""" + str(start_range) + """ AND rating <= """ + str(end_range) + """;"""
        cur.execute(command)
        openconnection.commit()

        start_range = end_range

        p = str(partitions[i]) + " created and data inserted"
        print p

    # Print the time taken to execute the range partitioning
    end = time.time()
    print "Time taken for range-partitioning is: ",end-start
    openconnection.commit()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    
    # Pre-check the variables:
    if (numberofpartitions <=0 or math.floor(numberofpartitions) != math.ceil(numberofpartitions) ):
        print " No of partitions is not an integer"
        return
    # Creating the number of rr partitions and printing them
    start = time.time()
    prefix = "rrobin_part"
    partitions = []
    for i in range(0, numberofpartitions):
        temp = prefix + str(i)
        partitions.append(temp)
    print partitions

    # Loop across each partition and insert data if its mod value matches the iter
    for i in range(0,numberofpartitions):
        # Create the partition table
        command = """CREATE TABLE """ + str(partitions[i]) + """ (userid INTEGER,movieid INTEGER,rating REAL);"""
        cur = openconnection.cursor()
        cur.execute(command)
        openconnection.commit()

        # Find the rowid of the records and take the mod value with num of partitions and insert in the required partition
        command = """INSERT INTO """ + str(partitions[i]) + """ (userid,movieid,rating) SELECT userid,movieid,rating FROM (SELECT row_number() over(), * FROM """ + ratingstablename + """ ) AS temp WHERE  (row_number - 1)%""" + str(numberofpartitions) + """ = """ + str(i) + """;"""
        cur.execute(command)
        openconnection.commit()
        p = str(partitions[i]) + " created and data inserted"
        print p

    # Print the time taken to execute the rr partitioning
    end = time.time()
    print "Time taken for roundrobin-partitioning is: ", end - start
    openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    # Precheck the values for error in limits
    if ( rating < 0 or rating>5 ):
        print "Rating cannot be negative or exceed 5"
        return
    # Insert the record into the ratingstablename table first
    start = time.time()
    command = """INSERT INTO """ + ratingstablename + """ (userid,movieid,rating) VALUES (""" + str(
        userid) + """,""" + str(itemid) + """,""" + str(rating) + """);"""
    cur = openconnection.cursor()
    cur.execute(command)
    openconnection.commit()
    print "Inserted the record in: ", ratingstablename

    # Get the count of rr_part<x> tables in the db and select where the data should be inserted
    command = """SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'rrobin%';"""
    cur.execute(command)
    openconnection.commit()
    num_partitions = cur.fetchone()

    # Get the row id of the record in ratingsfilename table
    command = """SELECT * FROM (SELECT row_number() over(), * FROM """ + ratingstablename + """) AS temp WHERE userid = """ + str(userid) + """ AND movieid = """ + str(itemid) +""" AND rating = """ + str(rating) + """;"""
    cur.execute(command)
    openconnection.commit()
    row_id = cur.fetchone()

    # Insert the record in the required rr partition accordingly using both the fetchOne values
    part = (row_id[0] - 1) % (num_partitions[0])
    part_file = "rrobin_part" + str(part)
    command = """INSERT INTO """ + part_file + """ (userid,movieid,rating) VALUES (""" + str(userid) + """,""" + str(itemid) + """,""" + str(rating) + """);"""
    cur.execute(command)
    openconnection.commit()
    print "Inserted into partition: ",part_file

    # Print the time taken to execute the rr insert query
    end = time.time()
    print "Time taken for rrobin-partitioning insert is: ",end-start
    openconnection.commit()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):

    # Precheck the values for error in limits
    if ( rating < 0 or rating>5 ):
        print "Rating cannot be negative or exceed 5"
        return
    # Insert the record into the ratingstablename table first
    start = time.time()
    command = """INSERT INTO """ + ratingstablename + """ (userid,movieid,rating) VALUES (""" + str(userid) + """,""" + str(itemid) + """,""" + str(rating) + """);"""
    cur = openconnection.cursor()
    cur.execute(command)
    openconnection.commit()
    print "Inserted the record in: ",ratingstablename

    # Get the count of range_part<x> tables in the db and select where the data should be inserted
    command = """SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'range%';"""
    cur.execute(command)
    openconnection.commit()
    num_partitions = cur.fetchone()

    # Brute force way to check if the rating falls in any partition or not and insert data
    prefix = "range_part"
    partitions = []
    for i in range(0, num_partitions[0]):
        temp = prefix + str(i)
        partitions.append(temp)
    start_range = 0
    end_range = 0
    incr = float(5) / num_partitions[0]
    for i in range(0,num_partitions[0]):
        end_range += incr
        #print start_range,end_range
        if (i == 0 and rating>=start_range and rating<=end_range):
            command = """INSERT INTO """ + str(partitions[i]) + """ (userid,movieid,rating) VALUES (""" + str(userid) + """,""" + str(itemid) + """,""" + str(rating) + """);"""
            break
        elif ( i!= 0 and rating>=start_range and rating<=end_range):
            command = """INSERT INTO """ + str(partitions[i]) + """ (userid,movieid,rating) VALUES (""" + str(userid) + """,""" + str(itemid) + """,""" + str(rating) + """);"""
            break
        else:
            start_range = end_range

    # The required partition found and data is inserted in that partition
    cur.execute(command)
    openconnection.commit()
    print "Inserted into partition: ",str(partitions[i])

    # Print the time taken to execute the range insert query
    end = time.time()
    print "Time taken for range-partitioning insertion is: ",end-start
    openconnection.commit()

def deletepartitionsandexit(openconnection):

    # Get the created tables and delete them
    cur = openconnection.cursor()
    command = """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"""
    cur.execute(command)
    openconnection.commit()
    tables = cur.fetchall()
    command = """DROP TABLE """
    for i in range(0,len(tables)):
        temp = command + str(tables[i][0]) + """;"""
        cur.execute(temp)
        openconnection.commit()
        print "Deleted table: ",tables[i][0]


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            con.autocommit = True
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings(ratingstablename,INPUT_FILE_PATH, con)
            rangepartition(ratingstablename,5,con)
            roundrobinpartition(ratingstablename,5,con)
            roundrobininsert(ratingstablename,100,1,3,con)
            rangeinsert(ratingstablename,100,2,3,con)
            #roundrobininsert('ratings',100,1,3,con)
            deletepartitionsandexit(con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
