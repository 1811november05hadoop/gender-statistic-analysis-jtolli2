package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChangeEmpFemaleMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		if(line.contains("SL.EMP.TOTL.SP.FE.ZS")) {
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
			if(count != 0){
				avg /= count;
				context.write(new Text("Female Average Employement Change"), new FloatWritable(avg));
			}
		}
	}
}
