#!/usr/bin/env python

import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import csv
import re

from datetime import datetime
from dateutil.parser import parse
import re

# Cleaning Borough (Column 23)
# Vales with "Unspecidief" are marked invalid.


## COde for Cleaning Borough


tests = [
    # (Type, Test)


    (int, int),
    (float, float),
    (datetime, lambda value: parse(value)),
    (str, str)
]

boroughSet=[
    "BROOKLYN",
    "MANHATTAN",
    "BRONX",
    "STATEN ISLAND",
    "QUEENS"
]
month31=[1,3,5,7,8,10,12]
month30=[4,6,9,11]
month28=[2]


## Code for cleaning Boroughs
def getValid(Borough):
    pattern = re.compile("^(?:[A-Za-z0-9 ])+$")
    combined = re.compile("(" + ")|(".join(boroughSet) + ")")
    if Borough != "" and pattern.match(Borough) and combined.match((Borough)):
        return True
    return False


def getBoroughType(x):
    #label = "invalid"
    myVal = getValid(x)
    if myVal:
        typ = str
    else:
        for typ, test in tests:
            try:
                test(x)
            except ValueError:
                continue
    # return(myVal)
    if not myVal:
        if x == '':
            label = 'N/A'
        else:
            label = "invalid"
        return label
    else:
        label = "valid"
    #typ = str(typ).replace('<class','').strip('>').strip(' ').strip('\'')
    return label


## Code Ends for Cleaning of Boroughs

#print(getBoroughType('MANHATTA'))


## Code For cleaning Date


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

            return label
        except ValueError:
            continue

    return label


## Code for Cleaning date ends

#print(getBoroughType('BROOKLYN'))
#print(getDateType('2015-02-02'))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: cleaning_date.py <file>")
        exit(-1)
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    csvfile = sc.textFile(sys.argv[1],1)

    ## Creating Agency RDD and Filtering only Valid Values
    key_val_borough = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[0], x[23]))
    result_borough = key_val_borough.filter(lambda x: getBoroughType(x[23]) == "valid")




    ## Creating Date RDD and Filtering only Valid Dates
    key_val_date = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[0], x[1]))
    result_date = key_val_date.filter(lambda x: getDateType(x[1]) == "valid")


    myDF1 = sqlContext.createDataFrame(key_val_borough, ('uni_key', 'BoroughName'))
    myDF2 = sqlContext.createDataFrame(key_val_date, ('uni_key', 'Date'))

    myDF1.createOrReplaceTempView("borough_view")
    myDF2.createOrReplaceTempView("date_view")

    finalRDD = sqlContext.sql(
        "SELECT borough_view.BoroughName, COUNT(*) AS Total_Complaints_Borough from borough_view JOIN date_view "
        "on borough_view.uni_key = date_view.uni_key "
        "GROUP BY borough_view.BoroughName")

    finalRDD.repartition(5).write.csv("sqlQuery3.csv", sep=',', header=True)

    sc.stop()

