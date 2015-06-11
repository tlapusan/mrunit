package org.sample.hadoop.mrunit;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobWordcount extends Configured implements Tool {

	public static final String JOB_NAME = JobWordcount.class.getSimpleName();

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JobWordcount(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.printf("Usage: %s [generic options] <input> <output> \n",
							JOB_NAME);
			ToolRunner.printGenericCommandUsage(System.err);
		}
		Job job = Job.getInstance(getConf());
		job.setJarByClass(JobWordcount.class);
		job.setMapperClass(MapWordcount.class);
		job.setReducerClass(ReduceWordcount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) == true ? 0 : 1;
	}
}
