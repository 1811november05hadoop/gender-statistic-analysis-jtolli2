package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.ChangeEmpFemaleMapper;
import com.revature.map.ChangeEmpMaleMapper;
import com.revature.map.FemaleGradCountryMapper;
import com.revature.map.USFemaleGradIncRateMapper;
import com.revature.map.USMaleGradIncRateMapper;
import com.revature.reduce.ChangeEmpFemaleReducer;
import com.revature.reduce.ChangeEmpMaleReducer;

public class GenderDataDriver {
	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.printf(
					"Usage: GenderDataDriver <map reduce option> <input dir> <output dir>\n"
					+ "Map Reduce Options:\n"
					+ "1: Question 1\n"
					+ "2: Question 2\n"
					+ "3: Question 3\n"
					+ "4: Question 4\n"
					+ "5: Question 5\n");
			System.exit(-1);
		}
		
		Job job;
		boolean success;
		switch (args[0]) {
		case "1":
			job = new Job();
			job.setJarByClass(GenderDataDriver.class);

			job.setJobName("Gender Data Driver 1");

			FileInputFormat.setInputPaths(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			
			job.setMapperClass(FemaleGradCountryMapper.class);
			job.setNumReduceTasks(0);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);

			success = job.waitForCompletion(true);
			System.exit(success ? 0 : 1);
			break;
		case "2":
			job = new Job();
			job.setJarByClass(GenderDataDriver.class);

			job.setJobName("Gender Data Driver 2");

			FileInputFormat.setInputPaths(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			
			job.setMapperClass(USFemaleGradIncRateMapper.class);
			job.setNumReduceTasks(0);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);

			success = job.waitForCompletion(true);
			System.exit(success ? 0 : 1);
			break;
		case "3":
			job = new Job();
			job.setJarByClass(GenderDataDriver.class);

			job.setJobName("Gender Data Driver 3");

			FileInputFormat.setInputPaths(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);
			
			job.setMapperClass(ChangeEmpMaleMapper.class);
			job.setReducerClass(ChangeEmpMaleReducer.class);

			success = job.waitForCompletion(true);
			System.exit(success ? 0 : 1);
			break;
		case "4":
			job = new Job();
			job.setJarByClass(GenderDataDriver.class);

			job.setJobName("Gender Data Driver 4");

			FileInputFormat.setInputPaths(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			
			job.setMapperClass(ChangeEmpFemaleMapper.class);
			job.setReducerClass(ChangeEmpFemaleReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);

			success = job.waitForCompletion(true);
			System.exit(success ? 0 : 1);
			break;
		case "5":
			job = new Job();
			job.setJarByClass(GenderDataDriver.class);

			job.setJobName("Gender Data Driver 5");

			FileInputFormat.setInputPaths(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			
			job.setMapperClass(USMaleGradIncRateMapper.class);
			job.setNumReduceTasks(0);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);

			success = job.waitForCompletion(true);
			System.exit(success ? 0 : 1);
			break;

		default:
			System.out.println("First argument must be 1-5");
			System.exit(-1);
			break;
		}
	}
}
