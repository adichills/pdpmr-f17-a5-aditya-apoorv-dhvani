# A5 - Routing

Given historical airplane on time performance data, this project offers suggestions
for two-hop flights that minimize the chance of missing a connection.

Pre-requisite softwares:
1. Java 8
2. Hadoop
3. R 

Run the project through terminal/linux shell by using the command "make".

Below are the necessary steps before that:
1. Copy the flight data and the input query to be processed to hdfs.
2. Update the variable FLIGHT_DATA with the name of the above flight data folder in the makefile.
3. Update the variable QUERY with the name of the above folder where input query is stored in the makefile.
4. Update variables HADOOP_HOME and HDFS_PATH as per your configuration.

Make sure that an out folder is there in the same path as src folder. 
Now run the "make" command. This will run the job as well as create a MainDriver.jar file.

For running on EC2 cluster, copy these files to the EC2 machine and follow the same steps as above.

If you want to run it on EMR, you can upload the .jar file. SSH to the machine, copy the input query to hdfs.
Run it on EMR by giving the first parameter as s3://pdpmrf17/, second parameter as hdfs location of input query file and 
third parameter as "counterfile". 

If you are not familiar with this, please follow the steps in the pdf attached.

Format of the input query (in a file):

2001,7,4,JFK,LAX
2001,7,4,BOS,LAX
2001,12,5,DEN,ORD

