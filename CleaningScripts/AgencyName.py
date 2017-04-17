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

def getDataType(x):
    label = "invalid"

    for typ, test in tests:
        try:
            test(x)
            if typ == "str":
                pattern = re.compile('^[A-Za-z\d\s\/-]+$')
                if pattern.match(x):
                    label = "valid"
                    break
                else:
                    if x == '':
                        label = "N/A"
                    else:
                        label = "invalid"
                    break;
            if typ == 'int':
                break
            if typ == 'float':
                break
            else:
                break
        except ValueError:
            continue

    return str(str(x)+', '+str(typ).replace('<class', '').strip('>')+', '+'Full Agency Name, '+label)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: AgencyName.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    agency = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[4])
    base_type=agency.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("AgencyName.txt")
    sc.stop()
