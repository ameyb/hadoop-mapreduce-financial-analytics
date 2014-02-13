package com.gsihadoop.minmaxavg;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.Text;

public class FinanceReducer52WeekLowWithDate extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	/**
	 * The `Reducer` method.
	 * 
	 * @param key
	 *            - Input key - Name of the region
	 * @param values
	 *            - Input Value - Iterator over the values
	 * @param context
	 *            - Used for collecting output
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {

		// Standard algorithm for finding the max value
		double low52week = Double.MAX_VALUE;
		for (DoubleWritable value : values) {
			low52week = Math.min(low52week, value.get());
		}

		context.write(key, new DoubleWritable(low52week));
	}
}

