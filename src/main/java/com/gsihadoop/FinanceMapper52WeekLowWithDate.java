package com.gsihadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinanceMapper52WeekLowWithDate extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	// public enum Counters { DataRowsWritten, DataInputErrors };
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
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		
		String userDateString = conf.get("date");
		
		Calendar userDate = Calendar.getInstance();
		Calendar previousDate = Calendar.getInstance();
		
		try {
			userDate.setTime(formatter.parse("2010-01-01"));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		previousDate.set(Calendar.YEAR, userDate.YEAR-1);
		previousDate.set(Calendar.MONTH, userDate.MONTH);
		previousDate.set(Calendar.DATE, userDate.DATE);

		String line = value.toString();
		

		StockData record = StockData.parse(line);

		if (record != null && !record.exchange.equals("exchange")) {
			Calendar recordDate = Calendar.getInstance();

			try {
				recordDate.setTime(formatter.parse(record.date));
			} catch (ParseException e) {
				e.printStackTrace();
			}
					
//			if ((recordDate.get(Calendar.YEAR) >= userDate.get(Calendar.YEAR) &&  
//					recordDate.get(Calendar.MONTH) >= userDate.get(Calendar.MONTH) &&
//					recordDate.get(Calendar.DAY_OF_MONTH) >= userDate.get(Calendar.DAY_OF_MONTH)) &&
//					(recordDate.get(Calendar.YEAR) <= previousDate.get(Calendar.YEAR) &&  
//					recordDate.get(Calendar.MONTH) <= previousDate.get(Calendar.MONTH) &&
//					recordDate.get(Calendar.DAY_OF_MONTH) <= previousDate.get(Calendar.DAY_OF_MONTH))
//					){
			if ((recordDate.compareTo(userDate) <= 0) && (recordDate.compareTo(previousDate) >= 0)){
				
				int year = recordDate.get(Calendar.YEAR);
				String outputKey = record.exchange + " " + record.stock_symbol
						+ " " + year;
				double outputValue = Double.parseDouble(record.stock_price_close);

				// Record the output in the Context object
				context.write(new Text(outputKey), new DoubleWritable(outputValue));
				    	
			}
			
			
		} else {
			// context.getCounter(Counters.DataInputErrors).increment(1);
		}

		// context.getCounter(Counters.DataRowsWritten).increment(1);
	}
}

