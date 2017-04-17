#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv
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
            if typ == float:
                pattern = re.compile('^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?)$')
                if pattern.match(x):
                    label = "valid"
                break

            if typ == int:
                break
            if typ == str and x == '':
                label="N/A"
                break
            else:
                break

        except ValueError:
            continue

    return str(str(x)+', '+str(typ).replace('<class', '').strip('>')+', '+'Latitude, '+label)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: Latitude.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    incidentAddress = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[49])
    base_type=incidentAddress.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("Latitude.txt")
    sc.stop()

