package com.hadoop.hlsijx.reducejoin.mapper;

import com.hadoop.hlsijx.reducejoin.model.JoinData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author hlsijx
 * 1、判断数据来源是emp还是dept
 * 2、设置输出对象及其来源标志
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, LongWritable, JoinData> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        if (fields.length > 3){
            StringBuilder data = new StringBuilder();
            data.append(fields[0]).append("\t")
                    .append(fields[1]).append("\t")
                    .append(fields[5]).append("\t");
            context.write(new LongWritable(Long.valueOf(fields[7])), new JoinData(data.toString(), 1));
        } else {
            context.write(new LongWritable(Long.valueOf(fields[0])), new JoinData(fields[1], 0));
        }
    }
}
