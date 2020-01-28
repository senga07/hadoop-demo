package com.hadoop.hlsijx.useragent.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author hlsijx
 */
public class UserAgentMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        String ua = fields[4];

        String operation = "-";
        if (ua.contains("Windows")){
            operation = "Windows";
        } else if (ua.contains("iPhone")){
            operation = "iPhone";
        } else if (ua.contains("Android")){
            operation = "Android";
        }
        context.write(new Text(operation), new LongWritable(1));
    }
}
