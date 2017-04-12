#!/usr/bin/env python

#scrip to clean the data

#import numpy as np

import sys
from pyspark import SparkContext
import csv
from datetime import datetime
import re


tests = [
   #(Type, Test)
    (int, int),
    (float, float),
    (datetime, lambda value: datetime.strptime(value, "%m/%d/%y")),
    (str, str)
]


regexes = [
    "N/A",
    "DSNY Garage",
    "Precinct",
    "School",
    "School District"
    ]


def getValidFacilityType(FacilityType):
    pattern = re.compile('[A-Z]|[a-z]|[0-9]|/|-| ')
    combined = re.compile("(" + ")|(".join(regexes) + ")")
    if FacilityType != "" and combined.match(FacilityType) and pattern.match(FacilityType):
        return True
    return False


def getDataType(x):
    label = "invalid"
    myVal = getValidFacilityType(x)
    if myVal:
        typ="str"
    else:
        for typ, test in tests:
            try:
                test(x)
            except ValueError:
                continue
    # return(myVal)
    if not myVal:
        label="invalid"
    else:
        label="valid"
    return str(x+', '+str(typ)+', '+ 'Facility Type, '+ label)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: Cleaning_FacilityType.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    unique_key = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[18])
    base_type=unique_key.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("FacilityType.txt")
sc.stop()
