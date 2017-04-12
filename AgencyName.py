#!/usr/bin/env python

# Cleaning column 4: Agency Name
import sys
from pyspark import SparkContext
import csv
from datetime import datetime
import re


tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d")),
    (str, str)
]


def getValidAgencyName(AgencyName):
    pattern = re.compile('^[A-Za-z\d\s\/-]+$')
    if AgencyName != "" and pattern.match(AgencyName):
        return True
    return False


def getDataType(x):
    #label = "invalid"
    myVal = getValidAgencyName(x)
    if myVal:
        typ = "str"
    else:
        for typ, test in tests:
            try:
                test(x)
            except ValueError:
                continue
    # return(myVal)
    if not myVal:
        label = "invalid"
    else:
        label = "valid"
    return str(x+', '+str(typ)+', '+'Full Agency Name, '+label)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: AgencyName.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    agency = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[4])
    base_type=agency.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("AgencyName.txt")
sc.stop()
