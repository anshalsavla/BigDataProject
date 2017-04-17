#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv

# Cleaning Cross Street 1 (Column 11)

import re

from datetime import datetime

tests = [
    # (Type, Test)
    (int, int),
    (float, float),
    (str, str),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d"))

]


def getValid(CrossStreet1):
    pattern = re.compile("^(?:[A-Za-z0-9 \.\/&,'\(\)\+-])+$")
    if CrossStreet1 != "" and pattern.match(CrossStreet1):
        return True
    return False



def getDataType(x):
    label = "invalid"

    for typ, test in tests:
        try:
            test(x)
            if typ == str:
                pattern = re.compile("^(?:[A-Za-z0-9 \.\/&,'\(\)\+-])+$")
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

    return str(str(x)+', '+str(typ).replace('<class', '').strip('>')+', '+'Cross Street No. 1, '+label)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: CrossStreet1.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    street = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[11])
    base_type=street.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("CrossStreet1.txt")
    sc.stop()
