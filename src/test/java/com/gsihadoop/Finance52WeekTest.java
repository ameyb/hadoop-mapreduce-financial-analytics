package src.test.java.com.gsihadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.gsihadoop.FinanceMapper52WeekHighWithDate;
import com.gsihadoop.FinanceReducer52WeekHigh;

public class Finance52WeekTest{

	MapDriver<LongWritable,Text,Text,DoubleWritable> mapDriver;
	ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	Configuration conf;
	
	@Before
	public void setUp(){
		FinanceMapper52WeekHighWithDate mapper = new FinanceMapper52WeekHighWithDate();
		mapDriver = MapDriver.newMapDriver(mapper);
		
		FinanceReducer52WeekHigh reducer = new FinanceReducer52WeekHigh();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		
		conf = new Configuration();
		conf.set("date","2009-08-08");
		mapDriver.setConfiguration(conf);
	}
	
	@Test
	public void testMapper() throws IOException{
		mapDriver.withInput(new LongWritable(), new Text("AMEX,AIP,2009-02-08,78.79,78.80,78.79,78.80,200,78.80"));
		//mapDriver.withOutput(new Text("ASDF"), new DoubleWritable(78.80f));
		mapDriver.withOutput(new Text("AIP AMEX"), new DoubleWritable(78.80f));
		mapDriver.withOutput(new Text("AIP AMEX"), new DoubleWritable(78.79f));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException{
		List<DoubleWritable> amex = new ArrayList<DoubleWritable>();
		//List<DoubleWritable> nasdaq = new ArrayList<DoubleWritable>();
		//List<DoubleWritable> nyse = new ArrayList<DoubleWritable>();
		amex.add(new DoubleWritable(79.80f));
		amex.add(new DoubleWritable(78.00f));
		amex.add(new DoubleWritable(79.81f));
		amex.add(new DoubleWritable(78.25f));
		amex.add(new DoubleWritable(79.00f));
		amex.add(new DoubleWritable(79.85f));
		amex.add(new DoubleWritable(76.77f));
		amex.add(new DoubleWritable(79.99f));
		
		reduceDriver.withInput(new Text("AIP AMEX"),amex);
		reduceDriver.withOutput(new Text("AIP AMEX"), new DoubleWritable(79.99f));
		reduceDriver.withOutput(new Text("AIP AMEX"), new DoubleWritable(76.77f));
		
		
		reduceDriver.runTest();
	}
}
