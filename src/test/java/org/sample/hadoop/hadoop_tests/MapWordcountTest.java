package org.sample.hadoop.hadoop_tests;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;
import org.sample.hadoop.mrunit.MapWordcount;

public class MapWordcountTest {

	@Test
	public void testMapCount() throws IOException {
		Pair<Text, IntWritable> outputPair = new Pair<Text, IntWritable>(
				new Text("sky"), new IntWritable(1));
		Pair<Text, IntWritable> outputPair1 = new Pair<Text, IntWritable>(
				new Text("is"), new IntWritable(1));
		Pair<Text, IntWritable> outputPair2 = new Pair<Text, IntWritable>(
				new Text("blue"), new IntWritable(1));
		new MapDriver<LongWritable, Text, Text, IntWritable>()
				.withMapper(new MapWordcount())
				.withInput(new LongWritable(0), new Text("sky is blue"))
				.withOutput(outputPair).withOutput(outputPair1)
				.withOutput(outputPair2).runTest();
	}
}
