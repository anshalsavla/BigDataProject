The following are the steps performed in order to run the scripts for hypothesis3:

ssh nyu_netid@hpc.nyu.edu //log on to NYU's HPC bastion host

ssh dumbo // log on to NYU's Hadoop node- dumbo

//To run hadoop jobs alias hfs='/usr/bin/hadoop fs ' export HAS=/opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib export HSJ=hadoop-mapreduce/hadoop-streaming.jar alias hjs='/usr/bin/hadoop jar $HAS/$HSJ'

//To load python 3.4.4 and set environment for spark to use python 3.4.4 module load python/gnu/3.4.4 export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python export PYTHONHASHSEED=0 export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

spark2-submit hypothesis3.py 311.csv

hfs -getmerge hyp3.csv hyp3.csv //Now after the jobs are done, get merged copy from hfs

Now import the output file to your local machine by using the following command-
scp netId@dumbo.es.its.nyu.edu:/path/hyp3.csv .

Following are the steps to plot the results on a graph-

python plotting2.py       //make sure this code and hyp3.csv that your imported from dumbo are in the same directory on your local machine
