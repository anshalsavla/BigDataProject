The following are the steps performed in order to run the scripts:
ssh nyu_netid@hpc.nyu.edu //log on to NYU's HPC bastion host
ssh dumbo // log on to NYU's Hadoop node- dumbo
//To run hadoop jobs alias hfs='/usr/bin/hadoop fs '
export HAS=/opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib
export HSJ=hadoop-mapreduce/hadoop-streaming.jar
alias hjs='/usr/bin/hadoop jar $HAS/$HSJ'

4. //To load python 3.4.4 and set environment for spark to use python 3.4.4 
module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python export PYTHONHASHSEED=0 export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

5. spark2-submit filename.py file.txt // to submit spark jobs, example: 'spark2-submit sqlQuery2.py 311.csv'

6. Now after the jobs are executed we need to perform: 
    hfs -getmerge sqlQueryX.csv sqlQueryX.csv 
  NOTE: X is the number of the Spark Task and ranges from 1-3.

   This command will enable to get the output file from the hfs to the working directory.

7. The file csv file now contains the result of the dataset and is feeded to any visualization tool such as MS Excel or Tableau.

8. If the csv files are needed on the local machine then simply perform scp command to ftech from dumbo to local machine.

scp netId@dumbo.es.its.nyu.edu:/path/to/directory .

