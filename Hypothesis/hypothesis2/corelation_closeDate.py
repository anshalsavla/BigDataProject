import csv
import sys
import numpy as np

def main():
    complaint=[]
    nodays=[]
    agency=[]
    f=open('complaintClosed_result.csv')
    data = csv.reader(f)
    next(data)
    for row in data:
        try:
            if row[0]=="HRA" or row[0]=="3-1-1" or row[0]=="NYPD":
                continue
            agency.append(row[0])
            complaint.append(int(row[1].strip('(')))
            nodays.append(int(row[2].strip(')')))
        except ValueError:
            continue
    print("Correlation coefficient for number of complaints to average number of days takes by department to close complaints.")
    print(np.corrcoef(complaint, nodays)[1, 0])





if __name__=='__main__':
    main()
