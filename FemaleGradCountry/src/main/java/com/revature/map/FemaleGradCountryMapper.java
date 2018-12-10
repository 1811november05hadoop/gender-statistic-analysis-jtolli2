package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FemaleGradCountryMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

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
		if(line.contains("SE.TER.CMPL.FE")) {
			String[] entry = line.split(",");
			float avg = 0;
			float count = 1;
			for (String string : entry) {
				String cleanString = string.substring(1, string.length()-1);
				//Only digits with decimals
				if(cleanString.matches("^\\d*\\.\\d+|\\d+\\.\\d*$")) {
					avg += Float.parseFloat(cleanString);
					count++;
				}
			}
			avg /= count;
			context.write(new Text(entry[0].substring(1, entry[0].length()-1)), new FloatWritable(avg));
		}
		
		/*if(line.matches("SE.TER.CMPL.FE")) {
			String[] entry = line.split(",");
			int avg = 0;
			for (String string : entry) {
				if(string.matches("\\d")) {
					int count = 1;
					avg = (avg + Integer.parseInt(string)) / count;
				}
			context.write(new Text(entry[0]), new IntWritable(avg));
			}
		}*/

		/*
		 * The line.split("\\W+") call uses regular expressions to split the
		 * line up by non-word characters.
		 * 
		 * If you are not familiar with the use of regular expressions in
		 * Java code, search the web for "Java Regex Tutorial." 
		 */
//		for (String word : line.split("\\W+")) {
//			if (word.length() > 3) {
//
//				/*
//				 * Call the write method on the Context object to emit a key
//				 * and a value from the map method.
//				 */
//				//Fix case sensitiveness only if the business allows this logic
//				context.write(new Text(word.toLowerCase()), new IntWritable(1));
//			}
//		}
	}
}
