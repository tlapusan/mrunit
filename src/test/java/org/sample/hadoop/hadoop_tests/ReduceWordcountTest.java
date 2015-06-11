package org.sample.hadoop.hadoop_tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;
import org.sample.hadoop.mrunit.ReduceWordcount;

public class ReduceWordcountTest {

	@Test
	public void testReduceSum() throws IOException {
		Text key = new Text("sky");
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(5));
		values.add(new IntWritable(4));
		values.add(new IntWritable(4));
		Pair<Text, List<IntWritable>> inputPair = new Pair<Text, List<IntWritable>>(
				key, values);
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
				.withReducer(new ReduceWordcount()).withInput(inputPair)
				.withOutput(new Text("sky"), new IntWritable(14)).runTest();
	}
}
