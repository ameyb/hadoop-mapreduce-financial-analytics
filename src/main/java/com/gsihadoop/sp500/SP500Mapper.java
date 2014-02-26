package com.gsihadoop.sp500;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.gsihadoop.utils.SP500List;
import com.gsihadoop.utils.StockData;

public class SP500Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context){
		Configuration conf = context.getConfiguration();
		
		Path path = Paths.get(conf.get("sp500Path"));
		
		String[] sp500List = SP500List.parseFile(path);
		String line = value.toString();
		
		try {
			StockData record = StockData.parse(line);
			String symbol = record.getStock_symbol();
			if (Arrays.binarySearch(sp500List, symbol) >= 0){
				String outputKey = symbol + "," + record.getDate().substring(0,4);
				String outputValue = record.getDate()+ "," + record.getStock_price_close();
				context.write(new Text(outputKey), new Text(outputValue));
			}
		} catch(ArrayIndexOutOfBoundsException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} 
	}
	
}
