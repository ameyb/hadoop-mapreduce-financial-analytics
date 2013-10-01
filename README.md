=======
## Hadoop Map-Reduce for Financial Analytics

### Instructions for Running the code
1. Clone the project:

	    $ git clone https://github.com/ameyb/hadoop-mapreduce-financial-analytics.git
	
2. Change to the project directory:

	    $ cd hadoop-mapreduce-financial-analytics

3. Build the project:

	    $ mvn clean install

4. Setup the HADOOP_CLASSPATH environment variable to tell Hadoop where to find the java classes for the sample:

	    $ export HADOOP_CLASSPATH=target/classes/

5. Copy the input data files to HDFS. The `output` directory shouldn't exists otherwise this will fail.

        $ hadoop jar target/hadoopfin-1.0.jar com.gsihadoop.App input/ output

> Note: the output will go to the `output/` folder which Hadoop will create when run. The output will be in a file called `part-r-00000`.

### Common Errors:
1. Exception: org.apache.hadoop.mapred.FileAlreadyExistsException or 'Output directory output already exists'. 
Cause: Output directory already exists. Hadoop requires that the output directory doesn't exists when run. 
Resolution: Change the output directory or remove the existing one:

        $ hadoop jar target/hadoopfin-1.0.jar com.gsihadoop.App input/input.csv output_new 

> Note: Hadoop failing if the output folder already exists is a good thing: it ensures that you don't accidentally overwrite your previous output, as typical Hadoop jobs take hours to complete.


