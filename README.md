
=======
## Hadoop Map-Reduce for Financial Analytics

### Instructions for Running the Sample
1. Clone the project:

	    $ git clone git@github.com:umermansoor/hadoop-java-example.git
	
2. Change to the project directory:

	    $ cd hadoop-java-example

3. Build the project:

	    $ mvn clean install

4. Setup the HADOOP_CLASSPATH environment variable to tell Hadoop where to find the java classes for the sample:

	    $ export HADOOP_CLASSPATH=target/classes/

5. Run the sample. The `output` directory shouldn't exists otherwise this will fail.

        $ hadoop com.umermansoor.App input/ output

> Note: the output will go to the `output/` folder which Hadoop will create when run. The output will be in a file called `part-r-00000`.

### Common Errors:
1. Exception: java.lang.NoClassDefFoundError
Cause: You didn't setup the HADOOP_CLASSPATH environment variable. You need to tell Hadoop where to find the java classes. 
Resolution: In this case, execute the following to setup HADOOP_CLASSPATH variable to point to the `target/classes/` folder.

        $ export HADOOP_CLASSPATH=target/classes/

2. Exception: org.apache.hadoop.mapred.FileAlreadyExistsException or 'Output directory output already exists'. 
Cause: Output directory already exists. Hadoop requires that the output directory doesn't exists when run. 
Resolution: Change the output directory or remove the existing one:

        $ hadoop com.umermansoor.App input/input.csv output_new 

> Note: Hadoop failing if the output folder already exists is a good thing: it ensures that you don't accidentally overwrite your previous output, as typical Hadoop jobs take hours to complete.

