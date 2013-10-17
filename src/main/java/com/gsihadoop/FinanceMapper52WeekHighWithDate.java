package com.gsihadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.LocalDate;
import com.gsi.calendar.DateUtilities;
import java.io.IOException;
import java.text.ParseException;

/**
 * This is the Mapper class used for calculating the 52 week high price of a
 * stock.
 * 
 * @author Jon D
 */
public class FinanceMapper52WeekHighWithDate extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {
	public enum Counters {
		DataRowsWritten, DataInputErrors
	};

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// Get the date parameter that was passed to the configuration object
		Configuration conf = context.getConfiguration();
		String dateString = conf.get("date");
		String line = value.toString();

		try {
			// Create the upper and lower boundary dates from the date parameter
			// passed in.
			// The date passed in becomes the upper bounds, and the lower is the
			// same date one year earlier.
			LocalDate upperBounds = DateUtilities.getJodaDate(dateString);
			LocalDate lowerBounds = upperBounds
					.withYear(upperBounds.getYear() - 1);

			// Parse the line and create a stock data object. Retrieve the
			// Joda-Time date from the record.
			StockData record = StockData.parse(line);
			LocalDate recordDate = DateUtilities.getJodaDate(record.getDate());

			// If the record's date is greater than or equal to the lower bounds
			// and the record's date
			// is less than or equal to the upper bounds, write the output key
			// value pair.
			if ((recordDate.compareTo(lowerBounds) >= 0)
					&& (recordDate.compareTo(upperBounds) <= 0)) {
				String outputKey = record.getStock_symbol() + " "
						+ record.getExchange();
				context.write(new Text(outputKey),
						new DoubleWritable(record.getStock_price_high()));
			}
			// Keep track of good records
			context.getCounter(Counters.DataRowsWritten).increment(1);

		} catch (ParseException pe) {
			// Keep track of failed comparisons
			context.getCounter(Counters.DataInputErrors).increment(1);
		}
	}
}