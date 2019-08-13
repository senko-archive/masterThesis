package com.senko.grep;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class GrepDriver {
	
	public static void main(String[] args) throws IOException, Exception, InterruptedException {
		
		// cli cpnfiguration
		Options options = new Options();
		Option inputPath = new Option("i", "input", true, "input folder of file");
		Option outputPath = new Option("o", "output", true, "output folder of file");
		inputPath.setRequired(true);
		outputPath.setRequired(true);
		
		options.addOption(inputPath);
		options.addOption(outputPath);
		
		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		
		try {
			cmd = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("utility-name", options);
			
			System.exit(1);
		}
		
		// create folder from path
		FileUtils.forceMkdir(new File(cmd.getOptionValue("input")));
		FileUtils.forceMkdir(new File(cmd.getOptionValue("output")));
		
		Path inputPathFromArgs = new Path(cmd.getOptionValue("input"));
		Path outputPathFromArgs = new Path(cmd.getOptionValue("output"));
		
		System.out.println("input path is: " + inputPathFromArgs);
		System.out.println("output path is: " + outputPathFromArgs);
		
	
		
		
		
		
		Path inputDirPath = inputPathFromArgs;
        Path outputDirPath = outputPathFromArgs;
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:/");
	    conf.set("mapreduce.framework.name", "local");  
		conf.set(GrepMapper.PATTERN, "aaa"); 
		
		System.out.println(conf.get(GrepMapper.PATTERN));
		
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(outputDirPath, true);
        fs.setWriteChecksum(false);
		
		Job job = Job.getInstance(conf);
		job.setJobName("grep task");
		job.setMapperClass(GrepMapper.class);
        job.setReducerClass(GrepReducer.class);
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job, inputDirPath);
        FileOutputFormat.setOutputPath(job, outputDirPath);
        System.out.println("so far everything is ok");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
	}


}
