package org.sample.hadoop.hadoop_tests;

import static org.hamcrest.CoreMatchers.is;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.sample.hadoop.mrunit.JobWordcount;

public class JobWordcountTest {

	@Test
	public void testJobDriver() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");

		Path input = new Path("data/input/data_wordcount.txt");
		Path output = new Path("data/output/output_wordcount.txt");

		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);

		JobWordcount driver = new JobWordcount();
		driver.setConf(conf);
		int exitCode = driver.run(new String[] { input.toString(),
				output.toString() });
		System.out.println("Exit code " + exitCode);
		Assert.assertThat(exitCode, is(0));
	}
}
