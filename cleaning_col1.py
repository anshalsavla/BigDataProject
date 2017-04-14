#!/usr/bin/env python

#script to check column 1-Unique Key

import numpy as np

import sys
from pyspark import SparkContext
import csv
from datetime import datetime

tests = [
   #(Type, Test)
    (int, int),
    (float, float),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d"))
]


def getDataType(x):
    label="invalid"
    for typ, test in tests:
        try:
            test(x)
            typ=str(typ).strip('<class').strip('>').strip(' ').strip('\'')
            if typ=="int":
                label="valid"
            val=typ+","+"Unique_Key,"+label

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
    unique_key = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[0])
    base_type=unique_key.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("Unique_Key.txt")
    sc.stop()
