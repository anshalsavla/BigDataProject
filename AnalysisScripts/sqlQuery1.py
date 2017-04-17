#!/usr/bin/env python

# scrip to Extract Information using SparkSQL Query

# import numpy as np

import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import csv
import re

from datetime import datetime
from dateutil.parser import parse

tests = [
    # (Type, Test)


    (int, int),
    (float, float),
    (datetime, lambda value: parse(value)),
    (str, str)
]




##Code Starts for Checking of Valid Agency Date
def getValidAgency(agency):
    patternmatch = re.compile("\\b[A-Z]{3}\\b|\\b[A-Z]{4}\\b|\\b[A-Z]{5}\\b")

    if patternmatch.match(agency):
        return True
    else:
        return False


def getAgencyType(x):
    label = "invalid"
    for typ, test in tests:
        try:
            test(x)

            typ = str(typ).strip('<type').strip('>').strip(' ').strip('\'')

            myVal = bool(getValidAgency(x))

            if myVal and typ == "str":
                label = "valid"

            return label
        except ValueError:
            continue
            # No match

    return label


## Code Ends for Checking of Valid Agency

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: cleaning_date.py <file>")
        exit(-1)
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    csvfile = sc.textFile(sys.argv[1], 1)

    ## Creating Agency RDD and Filtering only Valid Values
    key_val_agency = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[0], x[3]))
    result_agency = key_val_agency.filter(lambda x: getAgencyType(x[3]) == "valid")

    

    myDF1 = sqlContext.createDataFrame(key_val_agency, ('uni_key', 'AgencyName'))
   

    myDF1.createOrReplaceTempView("agency_view")
    

    finalRDD = sqlContext.sql(
        "SELECT COUNT(uni_key) AS Total_Complaints from agency_view"      
        )

    finalRDD.repartition(1).write.csv("sqlQuery1.csv", sep=',', header=True)

    sc.stop()


