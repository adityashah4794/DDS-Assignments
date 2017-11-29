#!/usr/bin/python2.7
#
# Assignment3 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
import math
import re

DATABASE_NAME = "ddsassignment5"
COLLECTION_NAME = "businessCollection"
CITY_TO_SEARCH = "tempe"
MAX_DISTANCE = 100
CATEGORIES_TO_SEARCH = ["Fashion", "Food", "Cafes"]
MY_LOCATION = [ "33.331229700000002","-111.642224"] #[LATITUDE, LONGITUDE]
SAVE_LOCATION_1 = "findBusinessBasedOnCity.txt"
SAVE_LOCATION_2 = "findBusinessBasedOnLocation.txt"

def distance(lat1,long1,lat2,long2):
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delphi = math.radians(lat2-lat1)
    dellambda = math.radians(long2-long1)

    a = math.sin(delphi/2) * math.sin(delphi/2) + math.cos(phi1) * math.cos(phi2) * \
        math.sin(dellambda/2) * math.sin(dellambda/2)
    c = 2 * math.atan2(math.sqrt(a),math.sqrt(1-a))
    return 3959*c

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    output = collection.find({"city":re.compile(cityToSearch,re.IGNORECASE)})
    result = open(saveLocation1,'w')
    for each in output:
        each["full_address"] = each["full_address"].replace("\n",", ")
        result.write(each["name"].encode('utf-8').encode('string_escape')+"$"+each["full_address"].encode('utf-8').encode('string_escape')+"$"+each["city"].encode('utf-8').encode('string_escape')+"$"+each["state"].encode('utf-8').encode('string_escape'))
        result.write("\n")
    result.close()
    #pass

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    output = collection.find({"categories" : {"$all" : categoriesToSearch }},{"categories":1,"name":1,"latitude":1,"longitude":1})
    result = open(saveLocation2,'w')
    latitude = float(myLocation[0])
    longitude = float(myLocation[1])
    tempList = []

    for each in output:
        lat = each.get("latitude")
        lon = each.get("longitude")
        if maxDistance >= distance(float(latitude),float(longitude),float(lat),float(lon)):
                tempList.append(each["name"].encode('utf-8'))

    for each in tempList:
        result.write(each)
        result.write("\n")
    
    result.close()
    collection.drop()
    #pass