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

def getValidAgency(agency):
    patternmatch = re.compile("\\b[A-Z]{3}\\b|\\b[A-Z]{4}\\b|\\b[A-Z]{5}\\b")

    if patternmatch.match(agency):
        return True
    else:
        return False



def getDataType(x):
    label="invalid"
    for typ, test in tests:
        try:
            test(x)

            typ=str(typ).strip('<type').strip('>').strip(' ').strip('\'')
            print typ
            myVal = bool(getValidAgency(x))
            print myVal
            if myVal and typ=="str":
                label="valid"
            val=typ+","+"Agency Name,"+label

            return x,val
        except ValueError:
            continue
     #No match
    val="str,Agency Name,invalid"
    return x,val





if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: cleaning_agency.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    unique_key = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[3])
    base_type=unique_key.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("agency.txt")
    sc.stop()
