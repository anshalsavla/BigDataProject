#!/usr/bin/env python


import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import csv
import re

from datetime import datetime
from dateutil.parser import parse

def getValidDate(value):
    try:
        a, b, c = value.split(" ")
        a = parse(a)
        a = a.strftime("%Y-%m-%d")
        return str(a)
    except ValueError:
        return "No Problem"


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: cleaning_date.py <file>")
        exit(-1)
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    csvfile = sc.textFile(sys.argv[1], 1)

    ## Creating RDD of Unique Key with Date
    key_val_date = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[0], getValidDate(x[1])))

    ## Creating RDD of Unique Key with Agency
    key_val_agency = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[0],x[3]))
    filterdAgency = key_val_agency.filter(lambda x: x[1] == "DOB" or  x[1] == "HPD")

    myDF1 = sqlContext.createDataFrame(key_val_date, ('uni_key', 'Date'))
    myDF2 = sqlContext.createDataFrame(filterdAgency, ('uni_key', 'Agency'))

    myDF1.createOrReplaceTempView("date_view")
    myDF2.createOrReplaceTempView("agency_view")

    result = sqlContext.sql("SELECT date_view.Date , COUNT(*) FROM date_view JOIN agency_view on "
                            "date_view.uni_key = agency_view.uni_key GROUP BY date_view.Date")

    result.repartition(5).write.csv("hyp3.csv", sep=',', header=True)