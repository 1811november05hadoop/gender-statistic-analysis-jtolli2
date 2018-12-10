package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.WordMapper;
import com.revature.reduce.SumReducer;

public class WordCountTest {
	
	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		WordMapper mapper = new WordMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		SumReducer reducer = new SumReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		
		//Mock Input
		mapDriver.withInput(new LongWritable(1), new Text("cats cats dogs"));
		
		//Expected Output
		mapDriver.withOutput(new Text("cats"), new IntWritable(1));
		mapDriver.withOutput(new Text("cats"), new IntWritable(1));
		mapDriver.withOutput(new Text("dogs"), new IntWritable(1));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		
		
		List<IntWritable> values = new ArrayList<>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		
		reduceDriver.withInput(new Text("cats"), values);
		
		reduceDriver.withOutput(new Text("cats"), new IntWritable(2));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		
		//Mock Input
		mapDriver.withInput(new LongWritable(1), new Text("cats cats dogs"));
		
		//Can't chain with addOutput
		//Expected Final Output
		reduceDriver
		.withOutput(new Text("cats"), new IntWritable(2))
		.withOutput(new Text("dogs"), new IntWritable(1));
	}
	
}