package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class is a Mapper that lists the average increase in female education in the U.S. from the year 2000 (considering only tertiary graduation).
 * It processes only the line with the indicator code 'SE.TER.CMPL.FE' and country code 'United States' and iterates through the received value to calculate the average for a specific country
 * @author cloudera
 *
 */
public class USFemaleGradIncRateMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		if(line.contains("SE.TER.CMPL.FE") && line.contains("United States")) {
			String[] entry = line.split(",");
			float avg = 0;
			float count = 0;
			float previous = 0;
			float current = 0;
			for (int i = entry.length-1-16; i < entry.length; i++) {
				String cleanString = entry[i].substring(1, entry[i].length()-1);
				//Only digits with decimals
				if(cleanString.matches("^\\d*\\.\\d+|\\d+\\.\\d*$")) {
					if(count > 0) {
						current = Float.parseFloat(cleanString);
						avg += (current - previous);
						previous = current;
					}
					else {
						current = Float.parseFloat(cleanString);
						previous = current;
					}
					count++;
				}
			}
			if(count != 0) {
				avg /= count;
				context.write(new Text(entry[0].substring(1, entry[0].length()-1)), new FloatWritable(avg));
			}
		}
	}
}
