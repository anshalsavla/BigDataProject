#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning SchoolState (Column 35)

import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d"))

]

def getDataType(x):
    label = "invalid"

    for typ, test in tests:
        try:
            test(x)
            if typ == str:
                pattern = re.compile("^(?:[A-Z])+$")
                if pattern.match(x):
                    label = "valid"
                    break
                else:
                    if x == '' or x == 'Unspecified':
                        label = "N/A"
                    else:
                        label = "invalid"
                    break;
            if typ == int:
                break
            if typ == float:
                break
            else:
                break

        except ValueError:
            continue

    return str(str(x)+', '+str(typ).replace('<class', '').strip('>')+', '+' SchoolState, '+label)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: SchoolState.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    school = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[35])
    base_type=school.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("SchoolState.txt")
sc.stop()

