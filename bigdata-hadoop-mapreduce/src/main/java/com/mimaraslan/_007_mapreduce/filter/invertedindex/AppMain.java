package com.mimaraslan._007_mapreduce.filter.invertedindex;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.NoSuchFileException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AppMain extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf());
		job.setJobName("invertedindex");
		job.setJarByClass(AppMain.class);

		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		// point to any folder with text files
		// Path myInputFilePath = new Path("/Users/lion/Documents/workspace/bigdata/hadoop/_001_Hadoop/src");
		// Path myOutputFilePath = new Path("/Users/lion/Documents/workspace/bigdata/hadoop/_001_Hadoop/data/output");

		String myInputFilePath = "data/input7";
		String myOutputFilePath = "data/output7";

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

		FileInputFormat.setInputDirRecursive(job, true);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AppMain(), args);
		System.exit(exitCode);
	}
}
