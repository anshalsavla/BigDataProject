#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning Intersection Street 1 (Column 13)

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
                pattern = re.compile("^(?:[A-Z0-9 \.\/&\'-])+$")
                if pattern.match(x):
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
    return str(str(x)+', '+str(typ).replace('<class', '').strip('>')+', '+'Intersection Street No. 1, '+label)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: IntersectionStreet1.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    street = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[13])
    base_type=street.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("IntersectionStreet1.txt")
sc.stop()
