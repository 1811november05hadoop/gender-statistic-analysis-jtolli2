package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class is a Mapper that identifies the countries where the % of female graduates is less than 30% (assuming tertiary graduation and taking every year of data available).
 * It processes only the lines with the indicator code 'SE.TER.CMPL.FE' and iterates through the received value to calculate the average for a specific country
 * @author cloudera
 *
 */
public class FemaleGradCountryMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		if(line.contains("SE.TER.CMPL.FE")) {
			String[] entry = line.split(",");
			float avg = 0;
			float count = 0;
			for (String string : entry) {
				String cleanString = string.substring(1, string.length()-1);
				//Only digits with decimals
				if(cleanString.matches("^\\d*\\.\\d+|\\d+\\.\\d*$")) {
					avg += Float.parseFloat(cleanString);
					count++;
				}
			}
			if(count != 0){
				avg /= count;
				if(avg <= 30 && avg != 0) {
					context.write(new Text(entry[0].substring(1, entry[0].length()-1)), new FloatWritable(avg));
				}
			}
		}
	}
}
