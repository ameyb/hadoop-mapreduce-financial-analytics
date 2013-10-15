package com.gsihadoop;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.gsi.calendar.CalendarWrapper;

public class Stock52WeekDriver {
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length != 3){
			System.err.println("Usage: 52WeekHighLow <input path> <output path> <YYYY-MM-DD>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		conf.set("date", args[2]);
		
		Job maxJob = new Job(conf);
		maxJob.setJarByClass(Stock52WeekDriver.class);
		maxJob.setJobName("52 Week Max");
		
		FileInputFormat.addInputPath(maxJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(maxJob, new Path(args[1]));
		
		maxJob.setMapperClass(FinanceMapper52WeekHighWithDate.class);
		//maxJob.setReducerClass(FinanceReducer52WeekHigh.class);
		
		maxJob.setOutputKeyClass(Text.class);
		maxJob.setOutputValueClass(FloatWritable.class);
		
		System.exit(maxJob.waitForCompletion(true) ? 0 : 1);
		
	}
}
