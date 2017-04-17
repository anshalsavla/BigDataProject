#!/usr/bin/env python

#scrip to clean the data

#import numpy as np

import sys
from pyspark import SparkContext
import csv
from dateutil.parser import parse
from datetime import datetime
import re


tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str,str),
    (datetime, lambda value: parse(value))
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

def getValidDate(entireVal):
    label = "Invalid"
    value, myTime, AMPM = entireVal.split(' ')
    m, d, y = value.split('-')
    if get_valid_DayMonth(m, d) and get_valid_year(y):
        return True
    else:
        return False




def getDataType(x):
    label="invalid"
    for typ, test in tests:
        try:
            test(x)

            typ=str(typ).strip('<type').strip('>').strip(' ').strip('\'')
            myVal = bool(getValidDate(x))
            if myVal:
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
    csvfile = sc.textFile(sys.argv[1],1)
    unique_key = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[1])
    base_type=unique_key.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("date1.txt")
    sc.stop()
