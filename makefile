INPUT = input
HADOOP_HOME = /home/aditya/hadoop/hadoop-2.8.1
MY_CLASSPATH = $(HADOOP_HOME)/share/hadoop/common/hadoop-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.1.jar:out:.
HDFS_PATH = /user/hadoop/

all: build run

build: compile jar

compile:
	javac -cp $(MY_CLASSPATH) -d out src3/*.java
jar:
	cp -r src2/META-INF/MANIFEST.MF out
	cd out; jar cvmf MANIFEST.MF MainDriver.jar *
	mv out/MainDriver.jar .

run:
	$(HADOOP_HOME)/bin/hadoop jar MainDriver.jar $(INPUT) inputFile/inputs counterFile

csv:
	mkdir output_csv_airline
	mkdir output_csv_airport
	$(HADOOP_HOME)/bin/hdfs dfs -get meanDelayOutputAirline/part* output_csv_airline
	$(HADOOP_HOME)/bin/hdfs dfs -get meanDelayOutputAirport/part* output_csv_airport
	mv output_csv_airline/part-* output_csv_airline/result_airline.csv
	mv output_csv_airline/result_airline.csv result_airline.csv
	rm -rf output_csv_airline
	mv output_csv_airport/part-* output_csv_airport/result_airport.csv
	mv output_csv_airport/result_airport.csv result_airport.csv
	rm -rf output_csv_airport
	Rscript -e 'rmarkdown::render("report.Rmd")'
