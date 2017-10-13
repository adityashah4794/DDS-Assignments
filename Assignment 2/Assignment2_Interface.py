#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    str1 = 'rangeratingspart'
    str2 = 'roundrobinratingspart'
    file = open("RangeQueryOut.txt","w+")

    cur = openconnection.cursor()
    cmd = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '" + str1 + "%';"
    cur.execute(cmd)
    openconnection.commit()
    
    range_part = cur.fetchone()[0]
    for i in range(0,range_part):
    	part = str1 + str(i)
    	cmd = "SELECT * FROM " + part + " WHERE Rating >= " + str(ratingMinValue) + " AND Rating <= " + str(ratingMaxValue) + ";"
    	cur.execute(cmd)
    	openconnection.commit()
    	values = cur.fetchall()
    	for each in values:
    		temp = str(part) + ',' + str(each[0]) + ',' + str(each[1]) + ',' + str(each[2]) + '\n'
    		file.write(temp)

    cmd = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '" + str2 + "%';"
    cur.execute(cmd)
    openconnection.commit()
    
    rrobin_part = cur.fetchone()[0]
    for i in range(0,rrobin_part):
    	part = str2 + str(i)
    	cmd = "SELECT * FROM " + part + " WHERE Rating >= " + str(ratingMinValue) + " AND Rating <= " + str(ratingMaxValue) + ";"
    	cur.execute(cmd)
    	openconnection.commit()
    	values = cur.fetchall()
    	for each in values:
    		temp = str(part) + ',' + str(each[0]) + ',' + str(each[1]) + ',' + str(each[2]) + '\n'
    		file.write(temp)
    file.close()    
    #Implement RangeQuery Here.
    #pass #Remove this once you are done with implementation

def PointQuery(ratingsTableName, ratingValue, openconnection):

	str1 = 'rangeratingspart'
	str2 = 'roundrobinratingspart'
	file = open("PointQueryOut.txt","w+")

	cur = openconnection.cursor()
	cmd = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '" + str1 + "%';"
	cur.execute(cmd)
	openconnection.commit()

	range_part = cur.fetchone()[0]
	for i in range(0,range_part):
		part = str1 + str(i)
		cmd = "SELECT * FROM " + part + " WHERE Rating = " + str(ratingValue) + ";"
		cur.execute(cmd)
		openconnection.commit()
		values = cur.fetchall()
		for each in values:
			temp = str(part) + ',' + str(each[0]) + ',' + str(each[1]) + ',' + str(each[2]) + '\n'
			file.write(temp)

	cmd = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '" + str2 + "%';"
	cur.execute(cmd)
	openconnection.commit()

	rrobin_part = cur.fetchone()[0]
	for i in range(0,rrobin_part):
		part = str2 + str(i)
		cmd = "SELECT * FROM " + part + " WHERE Rating = " + str(ratingValue) + ";"
		cur.execute(cmd)
		openconnection.commit()
		values = cur.fetchall()
		for each in values:
			temp = str(part) + ',' + str(each[0]) + ',' + str(each[1]) + ',' + str(each[2]) + '\n'
			file.write(temp)
	file.close()
	#Implement PointQuery Here.
	#pass # Remove this once you are done with implementation