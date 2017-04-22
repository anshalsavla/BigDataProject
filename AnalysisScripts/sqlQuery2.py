#!/usr/bin/env python

#script to query data

#import numpy as np

import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import csv
import re

from datetime import datetime
from dateutil.parser import parse

tests = [
    # (Type, Test)


    (int, int),
    (float, float),
    (datetime, lambda value: parse(value)),
    (str, str)
]

month31=[1,3,5,7,8,10,12]
month30=[4,6,9,11]
month28=[2]

#Code for Checking Valid Date

def get_valid_year(year_input):
    year = int(year_input)
    if 1900 < year < 2020:
        return True


def get_valid_DayMonth(month_input, day_input):
    month = int(month_input)
    day = int(day_input)
    if month in month28:
        if 0 < day < 29:
            return True
        return False
    if month in month30:
        if 0 < day < 31:
            return True
        return False
    if month in month31:
        if 0 < day < 32:
            return True
        return False


def getValidDate(value):
    value = parse(value)
    value = value.strftime("%Y-%m-%d %I:%M:%S")
    date, time = value.split(' ')
    y, m, d = date.split('-')
    if get_valid_DayMonth(m, d) and get_valid_year(y):
        return True
    else:
        return False


def getDateType(x):
    label = "invalid"
    for typ, test in tests:
        try:
            test(x)
            # print "Blah",typ
            typ = str(typ).replace('<class', '').strip('>').strip(' ').strip('\'')
            # print("Type is ", typ)
            myVal = bool(getValidDate(x))
            # print(myVal)
            if myVal and typ == "datetime.datetime":
                label = "valid"
            val = label
            return val
        except ValueError:
            continue
    val = "invalid"
    return val

##Code Ends for checking of Valid Date


##Code Starts for Checking of Valid Agency Date
def getValidAgency(agency):
    patternmatch = re.compile("\\b[A-Z]{3}\\b|\\b[A-Z]{4}\\b|\\b[A-Z]{5}\\b")

    if patternmatch.match(agency):
        return True
    else:
        return False





def getAgencyType(x):
    label="invalid"
    for typ, test in tests:
        try:
            test(x)

            typ=str(typ).strip('<type').strip('>').strip(' ').strip('\'')

            myVal = bool(getValidAgency(x))

            if myVal and typ=="str":
                label="valid"
            val=label

            return val
        except ValueError:
            continue
     #No match
    val="invalid"
    return val



## Code Ends for Checking of Valid Agency

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: cleaning_date.py <file>")
        exit(-1)
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    csvfile = sc.textFile("new_311.csv")

    ## Creating Agency RDD and Filtering only Valid Values
    key_val_agency = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[0], x[3]))
    result_agency = key_val_agency.filter(lambda x: getAgencyType(x[3]) == "valid")

    ## Creating Date RDD and Filtering only Valid Dates
    key_val_date = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[0], x[1]))
    result_date = key_val_date.filter(lambda x: getDateType(x[1]) == "valid")


    #RDDJoin = key_val_agency.join(key_val_date)

    #RDDJoin.saveAsTextFile('Joining.txt')

    myDF1 = spark.creteDataFrame(key_val_agency,('uni_key','AgencyName'))
    myDF2 = spark.creteDataFrame(key_val_date,('uni_key','Date'))

    myDF1.createOrReplaceTempView("agency_view")
    myDF2.createOrReplaceTempView("date_view")

    finalRDD = spark.sql("SELECT uni_key, AgencyName, Date from agency_view JOIN date_view "
                              "on agency_view.uni_key = date_view.un_key")


    finalRDD.repartition(1).write.csv("sqlQuery2.csv", sep='|')


    #a = sqlContext.sql("SELECT uni_key, Agency, Date FROM abc")
    #a.show()
    #a.repartition(1).write.csv("sqlQuery2.csv", sep='|')

    #a.saveAsTextFile("sqldata.csv")
    #a.write.format('com.databricks.spark.csv').save('sqlData.csv')
    sc.stop()

