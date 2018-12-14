package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChangeEmpMaleReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		float avg = 0;
		float count = 1;

		for (FloatWritable value : values) {
			avg = value.get() - avg;
			count++;
		}
		avg /= count;

		context.write(key, new FloatWritable(avg));
	}
}
