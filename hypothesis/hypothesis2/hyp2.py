#!/usr/bin/env python

#script to find correlation between no. of complaints per agency
#to avg no. of days it takes the agency to close the case.


import sys
from operator import add
from pyspark import SparkContext
import csv
from datetime import datetime
from dateutil.parser import parse


def toCSVLine(data):
  return ','.join(str(d) for d in data)


def getNoDays(startdate, enddate):
    try:

        date1 = parse(startdate)
        date2 = parse(enddate)
        print(date1,date2)
        difference = date2 - date1
        difference = str(difference)
        num, days, time = difference.split(' ')
        if int(num) < 0:
            return "NA"
        else:
            return int(num)

    except ValueError:
        return "NA"

def getAverage(value):
    #value=value.strip('(').strip(')')
    den = value[0]
    num = value[1]
    #den, num = value.split(',')
    avg = int(num)/int(den)
    return int(avg)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: hyp2.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1], 1)

    agency = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[3]))
    agencycount = agency.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    #csvresult = agencycount.map(toCSVLine)

    no_of_days = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[3],getNoDays(x[1],x[2])))
    filterednodays = no_of_days.filter(lambda x: x[1]!="NA")

    finalnodays = filterednodays.reduceByKey(lambda a, b: a + b)

    join = agencycount.join(finalnodays)
    averagerdd = join.map(lambda x:(x[0],getAverage(x[1])))
    result = agencycount.join(averagerdd)
    print = result.map(toCSVLine)
    print.saveAsTextFile("hyp2.csv")
    sc.stop()
