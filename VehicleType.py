#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning Vehicle Type (Column 39)
# Vales with "Unspecified" are marked invalid.

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
    "Ambulette / Paratransit",
    "Car Service",
    "Commuter Van"
]

def getDataType(x):
    label = "invalid"

    for typ, test in tests:
        try:
            test(x)
            if typ == str:
                combined = re.compile("(" + ")|(".join(regexes) + ")")
                if combined.match(x):
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

    return str(str(x)+', '+str(typ).replace('<class', '').strip('>')+', '+'Vehicle Type, '+label)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: VehicleType.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    boroughname = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[39])
    base_type=boroughname.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("VehicleType.txt")
sc.stop()
