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
    (str, str),
    (datetime, lambda value: datetime.strptime(value, "%m/%d/%y"))
]


regexes = [
    "N/A",
    "DSNY Garage",
    "Precinct",
    "School",
    "School District"
    ]



def getDataType(x):
    label = "invalid"

    for typ, test in tests:
        try:
            test(x)
            if typ == str:
                pattern = re.compile('[A-Z]|[a-z]|[0-9]|/|-| ')
                combined = re.compile("(" + ")|(".join(regexes) + ")")
                if combined.match(x) and pattern.match(x):
                    label = "valid"
                    break
                else:
                    if x == '':
                        label = "N/A"
                    else:
                        label = "invalid"
                    break;
        except ValueError:
            continue
    return str(x+', ' + str(typ).replace('<class', '').strip('>')+', ' + 'Facility Type, ' + label)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: Cleaning_FacilityType.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    unique_key = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[18])
    base_type=unique_key.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("FacilityType.txt")
    sc.stop()
