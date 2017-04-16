import sys
from pyspark import SparkContext
import csv
import re
from datetime import datetime


#Checking for valid Y-Coordinates

tests = [
    # (Type, Test)
    (str, str),
    (int, int),
    (float, float),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d"))

]

def getDataType(x):
    label = "invalid"

    for typ, test in tests:
        try:
            test(x)
            if typ == str:
                pattern = re.compile("\d{6}|\d{7}$")
                if (pattern.match(str(x))):
                    label = "valid"
                    break
                else:
                    if x == '' or x == 'Unspecified':
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

    return str(str(x)+', '+str(typ).replace('<class', '').strip('>')+', '+'Y-Coordinate, '+label)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: Ycoord.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile("new_311.csv")
    xc = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[25])
    base_type=xc.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("Ycoord.txt")
sc.stop()
