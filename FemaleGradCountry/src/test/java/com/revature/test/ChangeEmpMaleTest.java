package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.ChangeEmpMaleMapper;
import com.revature.reduce.ChangeEmpMaleReducer;

public class ChangeEmpMaleTest {
	
	private MapDriver<LongWritable, Text, Text, FloatWritable> mapDriver;
	private ReduceDriver<Text, FloatWritable, Text, FloatWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> mapReduceDriver;
	private Text line;
	
	@Before
	public void setUp() {
		ChangeEmpMaleMapper mapper = new ChangeEmpMaleMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		ChangeEmpMaleReducer reducer = new ChangeEmpMaleReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		
		line = new Text("\"Argentina\",\"ARG\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"72.4820022583008\",\"71.3369979858398\",\"70.0550003051758\",\"68.1269989013672\",\"63.3699989318848\",\"64.0169982910156\",\"65.5490036010742\",\"66.370002746582\","
				+ "\"64.8399963378906\",\"2.0\",\"2.0\",\"2.0\",\"2.0\",\"2.0\",\"2.0\",\"2.0\","
				+ "\"2.0\",\"2.0\",\"2.0\",\"2.0\",\"2.0\",\"2.0\",\"2.0\",\"2.0\","
				+ "\"2.0\",\"2.0\",");
	}
	
	@Test
	public void testMapper() {
		
		//Mock Input
		mapDriver.withInput(new LongWritable(1), line);
		
		//Expected Output
		mapDriver.withOutput(new Text("Male Average Employement Change"), new FloatWritable(0));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		
		
		List<FloatWritable> values = new ArrayList<>();
		values.add(new FloatWritable(1));
		values.add(new FloatWritable(1));
		
		reduceDriver.withInput(new Text("Male Average Employement Change"), values);
		
		reduceDriver.withOutput(new Text("Male Average Employement Change"), new FloatWritable(0));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		
		//Mock Input
		mapDriver.withInput(new LongWritable(1), line);
		
		//Can't chain with addOutput
		//Expected Final Output
		reduceDriver
		.withOutput(new Text("Male Average Employement Change"), new FloatWritable(0));
	}
	
}