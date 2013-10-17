package com.gsihadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.Text;

/**
 * This is the main Reducer class template. 
 * 
 * @author ameyb
 */
public class FinanceReducer extends 
        Reducer<Text, DoubleWritable, Text, DoubleWritable> 
{

    /**
     * The `Reducer` method.
     * @param key - Input key - Name of the region
     * @param values - Input Value - Iterator over the values
     * @param context - Used for collecting output
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, 
            Context context) throws IOException, InterruptedException {

        // Standard algorithm for finding the max value
        double High52week = Double.MIN_VALUE;
        for (DoubleWritable value : values) {
        	High52week = Math.max(High52week, value.get());
        }
        
        context.write(key, new DoubleWritable(High52week));
    }
}

