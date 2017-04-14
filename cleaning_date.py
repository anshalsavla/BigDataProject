#!/usr/bin/env python

#scrip to clean the data

#import numpy as np

import sys
from pyspark import SparkContext
import csv
import re
from datetime import datetime
from dateutil.parser import parse


tests = [
   #(Type, Test)


    (int, int),
    (float, float),
    (datetime, lambda value: parse(value)),
    (str,str)
]
month31=[1,3,5,7,8,10,12]
month30=[4,6,9,11]
month28=[2]


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
    print(value)
    date,time = value.split(' ')
    #print "Hello",value,time,hour
    y, m, d = date.split('-')
    if get_valid_DayMonth(m, d) and get_valid_year(y):
        return True
    else:
        return False




def getDataType(x):
    label="invalid"
    for typ, test in tests:
        try:
            test(x)
            #print "Blah",typ
            typ = str(typ).replace('<class', '').strip('>').strip(' ').strip('\'')
            print("Type is ", typ)
            myVal = bool(getValidDate(x))
            print(myVal)
            if myVal and typ=="datetime.datetime":
                label="valid"
            val=typ+","+"Date,"+label

            return x,val
        except ValueError:
            continue
     #No match
    val="str,Date,invalid"
    return x,val



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: cleaning_date.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    unique_key = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[1])
    base_type=unique_key.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("createdDate.txt")
    sc.stop()


