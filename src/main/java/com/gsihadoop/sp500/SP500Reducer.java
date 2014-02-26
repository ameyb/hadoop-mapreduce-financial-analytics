package com.gsihadoop.sp500;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.gsihadoop.utils.DateUtilities;

public class SP500Reducer extends Reducer<Text, Text, Text, DoubleWritable> {
	
	private MultipleOutputs multipleOutputs;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs  = new MultipleOutputs(context);
	}
	
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
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double aggregateYearClose = 0;
		double lowYear = Double.MAX_VALUE;
		
		String[] keyValue = (key.toString()).split(",");
		String symbol = keyValue[0];
		String year = keyValue[1];
		
		for (Text value : values) {
			String[] stringValues = (value.toString()).split(",");
			if (stringValues.length == 2){
				String dateString = stringValues[0];
				double close = Double.parseDouble(stringValues[1]);
				
				Calendar recordDate = Calendar.getInstance();
				try {
					recordDate.setTime(DateUtilities.getDate(dateString));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				
				
				//Year
				aggregateYearClose += close;
				
				// Week
				context.write(key, new DoubleWritable());
				
				//Month
				context.write(key, new DoubleWritable());
			}
		}
		// Year
		context.write(key, new DoubleWritable(aggregateYearClose));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
