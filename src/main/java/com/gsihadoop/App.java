package com.gsihadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

import com.gsihadoop.minmaxavg.FinanceMapper52WeekLowWithDate;
import com.gsihadoop.minmaxavg.FinanceReducer52WeekLowWithDate;
import com.gsihadoop.movingaverage.MovingAverageComparator;
import com.gsihadoop.movingaverage.MovingAverageMapper;
import com.gsihadoop.movingaverage.MovingAveragePartitioner;
import com.gsihadoop.movingaverage.MovingAverageReducer;
import com.gsihadoop.movingaverage.StockKey;

/**
 * Hadoop Financial Analytics Project
 * https://github.com/ameyb/hadoop-mapreduce-financial-analytics
 * 
 * @author ameyb
 */
public class App extends Configured implements Tool {
	// private static final Log LOG = LogFactory.getLog(App.class);
	private String inputPath, outputPath, functionName, functionDate,
			tableName = "";

	/**
	 * Application entry point.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new App(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();

		String[] otherArgs = (new GenericOptionsParser(conf, args))
				.getRemainingArgs();
		if (otherArgs.length < 4) {
			System.err
					.println("Usage: com.gsihadoop.App <input source location HDFS or S3> <hdfs output path or S3> <function> < optional function parameters>");
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}

		this.inputPath = otherArgs[0];
		this.outputPath = otherArgs[1];
		this.functionName = otherArgs[2];
		this.functionDate = otherArgs[3];

		conf.set("date", this.functionDate);
		
		Job job = new Job(conf);
		job.setJarByClass(App.class);
		job.setJobName("Hadoop Financial Analytics");

		// Set the Mapper, Reducer, Input/Out Key-value classes based on the function required
		if (functionName.equalsIgnoreCase("low") && functionDate != "") {
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			job.setMapperClass(FinanceMapper52WeekLowWithDate.class);
			job.setReducerClass(FinanceReducer52WeekLowWithDate.class);
		} else if (functionName.equalsIgnoreCase("high") && functionDate != "") {
			
		} else if (functionName.equalsIgnoreCase("simplemovingaverage") && functionDate != ""){
			job.setOutputKeyClass(StockKey.class);
			job.setOutputValueClass(DoubleWritable.class);
			job.setMapperClass(MovingAverageMapper.class);
			job.setReducerClass(MovingAverageReducer.class);
			job.setPartitionerClass(MovingAveragePartitioner.class);
			job.setGroupingComparatorClass(MovingAverageComparator.class);
			// Figure out why you will need this
			//job.setSortComparatorClass();
		}
		// Specify the type of output keys and values

		// Setup input path and inputformat
		FileInputFormat.addInputPath(job, new Path(inputPath));
		job.setInputFormatClass(TextInputFormat.class);

		// Setup output path and outputformat
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setOutputFormatClass(TextOutputFormat.class);

		// Execute Job and return status
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
