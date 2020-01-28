package com.hadoop.hlsijx.wordcount.mapper;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * @author hlsijx
 *
 * KEYIN：输入Key，一般为Long，是每行起始位置的偏移量，
 * VALUEIN：输入Value，一般为String，是每行字符串
 *
 * e.g.             KEYIN   VALUEIN
 * hello world      0       hello world
 * hello hadoop     10      hello hadoop
 *
 * KEYOUT：输出Key，一般为String
 * VALUEOUT：输出Value
 *
 * e.g.             KEYOUT  VALUEOUT
 * (hello,1)        hello   1
 * (world,1)        world   1
 * (hello,1)        hello   1
 * (hadoop,1)       hadoop  1
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //根据指定分割付切分行数据
        String regex = " ";
        String[] words = value.toString().split(regex);

        //给每一个单词赋值1
        for (String word : words) {
            context.write(new Text(word.toLowerCase()), new IntWritable(BigDecimal.ONE.intValue()));
        }
    }
}
