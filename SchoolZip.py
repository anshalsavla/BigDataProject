#!/usr/bin/env python

import sys
from pyspark import SparkContext
import csv
#scrip to clean the data in School Zip

import re
from datetime import datetime

tests = [
   #(Type, Test)
    (int, int),
    (float, float),
    (str,str),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d"))
]

def getValid(value):
    pattern = re.compile('^\d{5}(?:[-\s]\d{4})?$')
    if (pattern.match(str(value))):
        return True
    else:
        return False

def getDataType(x):
    label="invalid"
    for typ, test in tests:
        try:
            test(x)
            typ=str(typ).replace('<class','').strip('>').strip(' ').strip('\'')
            myVal = bool(getValid(x))
            if typ=="int" and myVal:
                label="valid"
            val=typ+", "+"ZipCode, "+label

            return x,val
        except ValueError:
            continue
     #No match
    val="str, Zip Code, invalid"
    return x,val


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: SchoolZip.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    zip = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[36])
    base_type=zip.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("SchoolZip.txt")
    sc.stop()


