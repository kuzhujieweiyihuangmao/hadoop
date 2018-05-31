package com.leslie;

import java.io.IOException;  
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount{  
	        public static class MyMapper extends Mapper<Object,Text,Text,IntWritable>{
	        		/*
	        		 * 建立Mapper类MyMapper继承自泛型类Mapper
	        		 * Mapper类：实现Map功能基类
	        		 * WritableComparable接口：实现WritableComparable的类可以相互比较。所有被用作key的类应该实现此接口。
	        		 * IntWritable, Text 均是 Hadoop 中实现的用于封装 Java 数据类型的类，这些类实现了WritableComparable接口，
	        		 * 都能够被串行化从而便于在分布式环境中进行数据交换，你可以将它们分别视为int,String 的替代品。
	        		 * 声明one常量和word用于存放单词的变量
	        		 */
	                private final static IntWritable one = new IntWritable(1);  
	                private Text word = new Text();  
	                public void map(Object key, Text value, Context context) throws IOException,InterruptedException{  
	                		/*
	                		 * Mapper中的map方法：
	                		 * void map(K1 key, V1 value, Context context)
	                		 * 映射一个单个的输入k/v对到一个中间的k/v对
	                		 * 输出对不需要和输入对是相同的类型，输入对可以映射到0个或多个输出对。
	                		 * Context：收集Mapper输出的<k,v>对。
	                		 * Context的write(k, v)方法:增加一个(k,v)对到context
	                		 * 主要编写Map和Reduce函数.这个Map函数使用StringTokenizer函数对字符串进行分隔,通过write方法把单词存入word中
	                		 * write方法存入(单词,1)这样的二元组到context中
	                		 */
	                        StringTokenizer itr = new StringTokenizer(value.toString());  
	                        while (itr.hasMoreTokens()){  
	                                word.set(itr.nextToken());  
	                                context.write(word,one);  
	                        }  
	                }  
	        } 
  
	        public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{ 
	                private IntWritable result = new IntWritable();  
	                public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
	                		/*
	                		 * Reducer类中的reduce方法：
	                		 * void reduce(Text key, Iterable<IntWritable> values, Context context)
	                		 * 中k/v来自于map函数中的context,可能经过了进一步处理(combiner),同样通过context输出 
	                		 */
	                        int sum = 0;  
	                        for (IntWritable val : values)  
	                        {  
	                                sum += val.get();  
	                        }  
	                        result.set(sum);  
	                        context.write(key,result);  
	                }  
	        }  
  
	        
	        public static void main(String[] args) throws Exception{  
	        		// Configuration：map/reduce的配置类，向hadoop框架描述map-reduce执行的工作
	                Configuration conf = new Configuration();  
	                conf.set("fs.default.name","hdfs://localhost:9000");
	        		String[] ioArgs = new String[]{"count_in", "count_out"};
	                String[] otherArgs = new GenericOptionsParser(conf,ioArgs).getRemainingArgs();  
	                if (otherArgs.length != 2)  
	                {  
	                        System.err.println("Usage: wordcount <in> <out>");  
	                        System.exit(2);  
	                }  
	                @SuppressWarnings("deprecation")
	                //设置环境参数
					Job job = new Job(conf,"word count");
	                //设置整个程序的类名
	                job.setJarByClass(WordCount.class);  
	                //添加MyMapper类
	                job.setMapperClass(MyMapper.class);  
	                //添加MyReducer类
	                job.setReducerClass(MyReducer.class);  	 
	                //设置输出类型
	                job.setOutputKeyClass(Text.class);  	
	                //设置输出类型
	                job.setOutputValueClass(IntWritable.class);   
	                //设置输入文件
	                FileInputFormat.addInputPath(job,new Path(otherArgs[0]));   
	                //设置输出文件
	                FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));  
	                //运行程序
	                int isSuccess = job.waitForCompletion(true) ? 0:1;
	                if (isSuccess == 0){
	                	System.out.println("操作执行成功！！！");
	                	System.exit(isSuccess);
	                }
	        }  
}  
 	
