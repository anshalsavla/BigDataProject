#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv
from datetime import datetime
from dateutil.parser import parse
import re


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
    date,time = value.split(' ')
    y, m, d = date.split('-')
    if get_valid_DayMonth(m, d) and get_valid_year(y):
        return True
    else:
        return False


def getDataType(x):
    label = "invalid"

    for typ, test in tests:
        try:
            test(x)
            if typ == str:
                if x == '':
                    label = "N/A"
                else:
                    label = "invalid"
                break;
            if getValidDate(x):
                label = "Valid"
                break


            if typ == int:
                break
            if typ == float:
                break
            else:
                break

        except ValueError:
            continue

    return str(str(x)+', '+str(typ).replace('<class', '').strip('>')+', '+'Closed Date, '+label)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: closedDate.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    dates = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[2])
    base_type=dates.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("closedDate.txt")
    sc.stop()

