package com.gsihadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This is the main Mapper class. 
 * 
 * @author ameyb
 */
public class FinanceMapper extends 
        Mapper<LongWritable, Text, Text, DoubleWritable> 
{

    /**
     * The `Mapper` method.
     * @param key - Input key - The line offset in the file - ignored.
     * @param value - Input Value - This is the line itself.
     * @param context - Provides access to the OutputCollector and Reporter.
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws 
            IOException, InterruptedException {
    	// Write your logic here
        String[] line = value.toString().split(",", 12); // 14 for usgs

        // Ignore invalid lines
        if (line.length != 12) {
            System.out.println("- " + line.length);
            return;
        }

        // The output `key` is the name of the region
        String outputKey = line[11]; // 10 for usgs

        // The output `value` is the magnitude of the earthquake
        double outputValue = Double.parseDouble(line[8]); // 4 for usgs
        
        // Record the output in the Context object
        context.write(new Text(outputKey), new DoubleWritable(outputValue));
    }
}

