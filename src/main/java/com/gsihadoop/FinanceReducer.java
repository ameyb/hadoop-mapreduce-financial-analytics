package com.gsihadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.Text;

/**
 * This is the main Mapper class. 
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
        // Write your logic here
        // Standard algorithm for finding the max value
        double maxMagnitude = Double.MIN_VALUE;
        for (DoubleWritable value : values) {
            maxMagnitude = Math.max(maxMagnitude, value.get());
        }
        
        context.write(key, new DoubleWritable(maxMagnitude));
    }
}

