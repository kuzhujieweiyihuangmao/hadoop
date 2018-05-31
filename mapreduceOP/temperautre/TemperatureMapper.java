package com.leslie.temperautre;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Integer ERROR_TEMPER = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String content = value.toString();
        String year = content.substring(15, 19);    //获取year

        Integer temperature  = null;    //获取温度
        if('+' == content.charAt(45)) {
            temperature = Integer.parseInt(content.substring(46, 50));
        } else {
            temperature = Integer.parseInt(content.substring(45, 50));
        }

        if(temperature <= ERROR_TEMPER && content.substring(50, 51).matches("[01459]")) {
            context.write(new Text(year), new IntWritable(temperature));
        }
    }

}