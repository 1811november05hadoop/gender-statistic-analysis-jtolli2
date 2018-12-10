package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USFemaleGradIncRateMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	/*
	 * The map method runs once for each line of text in the input file.
	 * The method receives a key of type LongWritable, a value of type
	 * Text, and a Context object.
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/*
		 * Convert the line, which is received as a Text object,
		 * to a String object.
		 */
		String line = value.toString();

		//also if(line.contains(".*(SE.TER.CMPL.FE).*"))
		if(key.get() == 158444l) {
			String[] entry = line.split(",");
			float avg = 0;
			float count = 1;
			for (String string : entry) {
				String cleanString = string.substring(1, string.length()-1);
				//Only digits with decimals
				if(cleanString.matches("^\\d*\\.\\d+|\\d+\\.\\d*$")) {
					avg = Math.abs(Float.parseFloat(cleanString) - avg);
					count++;
				}
			}
			avg /= count;
			context.write(new Text(entry[0].substring(1, entry[0].length()-1)), new FloatWritable(avg));
		}
	}
}
