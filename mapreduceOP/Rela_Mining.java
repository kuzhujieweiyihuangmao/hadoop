package com.leslie;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Rela_Mining{
	public static class Map extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) 
						throws IOException,InterruptedException{
			String childName = new String();
			String parentName = new String();
			String relationType = new String();
			String line = value.toString();
			String[] values = line.split(" ");
			if (values[0].compareTo("child") != 0){
				childName = values[0];
				parentName = values[1];
				relationType = "1";		//左表标志
				context.write(new Text(parentName), new Text(relationType + " " + childName));
				relationType = "2";		//右表标志
				context.write(new Text(childName), new Text(relationType + " " + parentName));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) 
						throws IOException,InterruptedException{
			String[] grandChild = new String[10];	//存放孙子的数组
			int grandChildNum = 0;
			String[] grandParent = new String[10];	//存放祖辈的数组
			int grandParentNum = 0;
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				String[] record = it.next().toString().split(" ");
				if(record.length==0)
					continue;
				if (record[0].equals("1")) {	//孙子放到一个数组里
					grandChild[grandChildNum] = record[1];
					grandChildNum++;
				}else {		//祖辈放到另一个数组中
				grandParent[grandParentNum] = record[1];
				grandParentNum++;
			}
		}
		if (grandChildNum != 0 && grandParentNum != 0) {
			//两个数组的X值为grandChild-grandParent关系
			for (int i = 0; i < grandChildNum; i++) {
				for (int j = 0; j < grandParentNum; j++) {
					context.write(new Text(grandChild[i]), new Text(grandParent[j]));
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String[] otherArgs = new String[]{"relation_in", "relation_out"};
		if (otherArgs.length != 2){
			System.err.println("Usage: Single Table Join <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Single table Join");
		job.setJarByClass(Rela_Mining.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		int isSuccess = job.waitForCompletion(true) ? 0 : 1;
		if (isSuccess == 0){
			System.out.println("操作执行成功!");
			System.exit(isSuccess);
		}
	}
}