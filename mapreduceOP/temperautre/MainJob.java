package com.leslie.temperautre;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainJob {
    public static void main(String[] args) throws Exception {

    	Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String[] otherArgs = new String[]{"weather_in", "weather_out"};
		if (otherArgs.length != 2){
			System.err.println("Usage: Single Table Join <in> <out>");
			System.exit(2);
		}
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "temperature");
        job.setJarByClass(MainJob.class);

        job.setMapperClass(TemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		int isSuccess = job.waitForCompletion(true) ? 0 : 1;
		if (isSuccess == 0){
			System.out.println("操作执行成功!");
			System.exit(isSuccess);
		}
	}
}