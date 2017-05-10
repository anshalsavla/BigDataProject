import csv
import sys
import numpy as np


def main():
    complaint=[]
    temperature=[]
    f=open('hyp1.csv')
    data = csv.reader(f)
    next(data)
    for row in data:
        try:
            complaint.append(int(row[1]))
            temperature.append(int(row[2]))
        except ValueError:
            continue
    print("Correlation coefficient for number of heating complaints to temperature")
    print(np.corrcoef(complaint, temperature)[1, 0])

if __name__=='__main__':
    main()