#!/usr/bin/env python

# scrip to Extract Information using SparkSQL Query

import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import csv
import re

from datetime import datetime
from dateutil.parser import parse

def getValidDate(value):
    try:
        a,b,c=value.split(" ")
        a = parse(a)
        a = a.strftime("%Y-%m-%d")
        return str(a)
    except ValueError:
        return "No Problem"

def getValidWeatherDate(value):
    try:
        a = parse(value)
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

    csvfile2 = sc.textFile(sys.argv[2],1)

    ## Creating RDD FOR Complains and Date
    key_val_complains = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[0], x[5]))
    key_val_date = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[0], getValidDate(x[1])))

    key_rdd2 = csvfile2.mapPartitions(lambda x: csv.reader(x)).map(lambda x: (getValidWeatherDate(x[1]),x[2]))

    

    myDF1 = sqlContext.createDataFrame(key_val_date, ('uni_key', 'Date'))
    myDF2 = sqlContext.createDataFrame(key_val_complains, ('uni_key', 'Complains'))

    myDF3 = sqlContext.createDataFrame(key_rdd2,('w_date','min_temp'))


    myDF1.createOrReplaceTempView("date_view")
    myDF2.createOrReplaceTempView("complain_view")

    myDF3.createOrReplaceTempView("weather_view")



    finalRDD = sqlContext.sql(
        "SELECT date_view.Date,COUNT(*) AS Total_Complaints from complain_view JOIN date_view "
        "on complain_view.uni_key = date_view.uni_key where complain_view.Complains=\'HEATING\' "
        "GROUP BY date_view.Date")

    #jointDF = sqlContext.createDataFrame(finalRDD,('Date','Total_Complaints'))
    finalRDD.createOrReplaceTempView("firstJoint")

    lastRDD = sqlContext.sql(
        "SELECT firstJoint.Date, firstJoint.Total_Complaints, weather_view.min_temp "
        "from firstJoint inner join weather_view on firstJoint.Date = weather_view.w_date "
        )

    lastRDD.repartition(5).write.csv("hyp1.csv", sep=',', header=True)

    sc.stop()


