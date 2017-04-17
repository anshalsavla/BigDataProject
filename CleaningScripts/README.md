Authors: Anshal Savla, Drumil Bakhai, Tannavee Watts
Project: BigData-Part1 (Dataquailty scripts)
Data: 311 complaints dataset from NYC Open Datasets -   https://data.cityofnewyork.us/Social-Services/311/wpe2-h2i5
Data Format: csv

This Directory (CleaningScripts) contains 52 scripts to find data quality issues in each of the 52 columns in the 311 data set.

Each script generates an text file returning the base type, semantic type and lable for each value in the column.
The output looks like :

    ===========================================================================
    |          BaseType         |     Semantic Type        |      Label       |
    |---------------------------|--------------------------|------------------|
    |int,float, string, datetime|Key, Address, Phonenumber,|Valid, Invalid,   |
    |                           |Zipcode, Location,DateTime|N/A               |
    ===========================================================================

The report at :https://docs.google.com/document/d/1ylkNHauQbcB5xerP8VaqJcV20-Ads-gr1aYEoQJ2Gt0/edit?usp=sharing gives a detaied ddesciption of the data quality issues found with the dataset.

Dependencies:
Hadoop distribution Cloudera CDH 5.9.0 (Hadoop 2.6.0 with Yarn)
Spark version 2.0
Python version 3.4.4
Python numpy library
Python dateutil.parser library
Python datetime library
Python sys library
Pyhton re library
Python csv library
Pyhthon SparkContext library
Pyhton Pyspark library

The following are the steps performed in order to run the scripts:

1. ssh nyu_netid@hpc.nyu.edu       //log on to NYU's HPC bastion host
2. ssh dumbo                       // log on to NYU's Hadoop node- dumbo
3. //To run hadoop jobs
   alias hfs='/usr/bin/hadoop fs '\
   export HAS=/opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib\
   export HSJ=hadoop-mapreduce/hadoop-streaming.jar\
   alias hjs='/usr/bin/hadoop jar $HAS/$HSJ'

4. //use this command in the local worksation direcorty where the file is located to load it on to your dumbo working directory.
   scp <filename.type> NYU-netid@dumbo.es.its.nyu.edu:/<working directory on dumbo>      
5. hfs -put <filename.type>        // this loads the data on HDFS
6. //To load python 3.4.4 and set environment for spark to use python 3.4.4 \
   module load python/gnu/3.4.4    
   export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
   export PYTHONHASHSEED=0
   export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0  
7. spark2-submit filename.py file.txt   // to submit spark jobs, example: 'spark2-submit UniqueKey.txt 311.csv'
8. hfs -getmerge filename.txt           //to view the result, get the merged file on your dumbo directory.
  
