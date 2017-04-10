#!/usr/bin/env python

#scrip to clean the data

#import numpy as np

import sys
from pyspark import SparkContext
import csv
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
            typ=str(typ).strip('<type').strip('>').strip(' ').strip('\'')
            myVal = bool(getValid(x))
            if (typ=="int" or typ=="str") and myVal:
                label="valid"
            val=typ+","+"ZipCode,"+label

            return x,val
        except ValueError:
            continue
     #No match
    val="str,Unique_Key,invalid"
    return x,val


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: cleaning.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    unique_key = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[8])
    base_type=unique_key.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("zipcode.txt")
    #base_type.collect()
    sc.stop()
