package com.gsihadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * This is the main Mapper class template.
 * 
 * @author ameyb
 */
public class FinanceMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {

	private String inputPath, outputPath;

	public enum Counters {
		DataRowsWritten, DataInputErrors
	};

	protected void setup(Context context) throws IOException,
			InterruptedException {
		this.inputPath = context.getConfiguration().get("inputpath");
		this.outputPath = context.getConfiguration().get("outputpath");
	}

	/**
	 * The `Mapper` method.
	 * 
	 * @param key
	 *            - Input key - The line offset in the file - ignored.
	 * @param value
	 *            - Input Value - This is the line itself.
	 * @param context
	 *            - Provides access to the OutputCollector and Reporter.
	 * @throws IOException
	 * @throws InterruptedException
	 */

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String rowKey = "";
		
		// This will keep your output for min/max
		double doubleValue = 0;
		
		String line = value.toString();
		// Record the output in the Context object
		String[] valueArray = line.split(",", -1);
		if (valueArray.length != 0) {
			// Add logic here
		} else {
			context.getCounter(Counters.DataInputErrors).increment(1);
			throw new RuntimeException("Invalid input line detected");
		}
		context.getCounter(Counters.DataRowsWritten).increment(1);
		context.write(new Text(rowKey), new DoubleWritable(doubleValue));
	}
}
