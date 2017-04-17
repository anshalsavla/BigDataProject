#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning SchoolRegion (Column 27)
# Values with "Unspecified" are marked valid.

import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d"))

]

regexes=[
    "Alternative Superintendency",
    "District 75",
    "Region 1",
    "Region 2",
    "Region 3",
    "Region 4",
    "Region 5",
    "Region 6",
    "Region 7",
    "Region 8",
    "Region 9",
    "Region 10",
    "Unspecified"
]

def getDataType(x):
    label = "invalid"

    for typ, test in tests:
        try:
            test(x)
            if typ == str:
                pattern = re.compile("^(?:[A-Za-z0-9 ])+$")
                combined = re.compile("(" + ")|(".join(regexes) + ")")
                if pattern.match(x) and combined.match(x):
                    label = "valid"
                    break
                else:
                    if x == '':
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

    return str(str(x)+', '+str(typ).replace('<class', '').strip('>')+', '+'School Region, '+label)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: SchoolRegion.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    school = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[30])
    base_type=school.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("SchoolRegion.txt")
    sc.stop()
