The following are the steps performed in order to run the scripts for hypothesis2:

log on to NYU's HPC bastion host: <br >
ssh nyu_netid@hpc.nyu.edu 

log on to NYU's Hadoop node- dumbo <br >
ssh dumbo 

//To run hadoop jobs <br >
alias hfs='/usr/bin/hadoop fs ' <br >
export HAS=/opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib <br >
export HSJ=hadoop-mapreduce/hadoop-streaming.jar alias hjs='/usr/bin/hadoop jar $HAS/$HSJ'<br >

//To load python 3.4.4 and set environment for spark to use python 3.4.4 <br >
module load python/gnu/3.4.4 <br >
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python  <br > 
export PYTHONHASHSEED=0  <br >
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0  <br >

Submit the job:  <br >
spark2-submit hyp2.py 311.csv

Now after the jobs are done, get merged copy from hfs <br >
hfs -getmerge hyp2.csv hyp2.csv 

Now import the output file to your local machine by using the following command: <br >  
scp netId@dumbo.es.its.nyu.edu:/path/hyp2.csv .

Following are the steps to find the correlation factor-

python corelation_closeDate.py.  //make sure the code and hyp2.csv that your imported from dumbo are in the same directory on your local machine
