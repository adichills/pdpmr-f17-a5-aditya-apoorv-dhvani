FLIGHT_DATA = input
QUERY = inputFile/inputs
HADOOP_HOME = /home/aditya/hadoop/hadoop-2.8.1
MY_CLASSPATH = $(HADOOP_HOME)/share/hadoop/common/hadoop-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.1.jar:out:.
HDFS_PATH = /user/hadoop/

all: build run

build: compile jar

compile:
	javac -cp $(MY_CLASSPATH) -d out src/*.java
jar:
	cp -r src/META-INF/MANIFEST.MF out
	cd out; jar cvmf MANIFEST.MF MainDriver.jar *
	mv out/MainDriver.jar .

run:
	$(HADOOP_HOME)/bin/hadoop jar MainDriver.jar $(FLIGHT_DATA) 	$(QUERY) counterFile
	Rscript -e 'rmarkdown::render("report.Rmd")'
