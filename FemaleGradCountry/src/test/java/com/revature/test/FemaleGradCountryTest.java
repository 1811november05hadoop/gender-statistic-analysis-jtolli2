package com.revature.test;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.FemaleGradCountryMapper;

public class FemaleGradCountryTest {
	
	private MapDriver<LongWritable, Text, Text, FloatWritable> mapDriver;
	
	@Before
	public void setUp() {
		FemaleGradCountryMapper mapper = new FemaleGradCountryMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
	}
	
	@Test
	public void testMapper() {
		
		//Mock Input
		mapDriver.withInput(new LongWritable(1), new Text("\"Bahrain\",\"BHR\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"1.0\",\"\",\"1.0\",\"1.0\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));
		
		//Expected Output
		mapDriver.withOutput(new Text("Bahrain"), new FloatWritable(1));
		
		mapDriver.runTest();
	}
}