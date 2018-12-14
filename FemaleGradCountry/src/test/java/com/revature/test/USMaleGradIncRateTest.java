package com.revature.test;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.USMaleGradIncRateMapper;

public class USMaleGradIncRateTest {

	private MapDriver<LongWritable, Text, Text, FloatWritable> mapDriver;

	@Before
	public void setUp() {
		USMaleGradIncRateMapper mapper = new USMaleGradIncRateMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
	}

	@Test
	public void testMapper() {

		//Mock Input
		mapDriver.withInput(new LongWritable(3), new Text("\"United States\",\"USA\",\"Gross graduation ratio, tertiary, male (%)\",\"SE.TER.CMPL.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"8.0\",\"\",\"8.0\",\"8.0\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));

		//Expected Output
		mapDriver.withOutput(new Text("United States"), new FloatWritable(0));

		mapDriver.runTest();
	}
}