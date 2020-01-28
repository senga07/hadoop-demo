package com.hadoop.hlsijx.access.mapper;

import com.hadoop.hlsijx.access.model.Access;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author hlsijx
 * 流量日志统计分析，日志数据位置在txt/access/access.log
 * 知识点：自定义对象
 */
public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fileds = value.toString().split("\t");
        String mobile = fileds[1];
        String up = fileds[fileds.length - 2];
        String down = fileds[fileds.length - 1];

        context.write(new Text(mobile), new Access(mobile, Long.valueOf(up), Long.valueOf(down)));
    }
}
