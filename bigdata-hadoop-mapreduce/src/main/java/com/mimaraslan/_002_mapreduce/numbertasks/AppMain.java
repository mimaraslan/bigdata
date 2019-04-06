package com.mimaraslan._002_mapreduce.numbertasks;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.NoSuchFileException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AppMain extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		Job job = Job.getInstance(conf);
		job.setJobName("viewCount");
		job.setJarByClass(AppMain.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(2);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// job.setCombinerClass(Reduce.class);

		String myInputFilePath = "data/input2/census.txt";
		String myOutputFilePath = "data/output2";
		
		try {
			FileUtils.deleteDirectory(new File(myOutputFilePath));
		} catch (NoSuchFileException x) {
			System.err.format("%s: no such" + " file or directory%n", myOutputFilePath);
		} catch (DirectoryNotEmptyException x) {
			System.err.format("%s not empty%n", myOutputFilePath);
		} catch (IOException x) {
			System.err.println(x);
		}

		Path inputFilePath = new Path(myInputFilePath);
		Path outputFilePath = new Path(myOutputFilePath);
		// Path inputFilePath = new Path(args[0]);
		// Path outputFilePath = new Path(args[1]);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new AppMain(), args);
		System.exit(exitCode);
	}
}
