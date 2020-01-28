package com.hadoop.hlsijx.wordcount.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author hlsijx
 *
 * KEYIN：输入Key，Mapper的输出KEYIN
 * VALUEIN：输入Value，Mapper的输出VALUEIN
 *
 * e.g.             KEYIN   VALUEIN
 * (hello,1)        hello   1
 * (world,1)        world   1
 * (hello,1)        hello   1
 * (hadoop,1)       hadoop  1
 *
 * KEYOUT：输出Key
 * VALUEOUT：输出Value
 *
 * e.g.             KEYOUT  VALUEOUT
 * (hello,2)        hello   2
 * (world,1)        world   1
 * (hadoop,1)       hadoop  1
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * map输出到reduce端时，是按照相同的key分发到同一个reduce上去
     * e.g.
     * reduce1:(hello,1),(hello,1) ==> (hello, <1,1>)
     * reduce2:(world,1) ==> (world,<1>)
     * reduce3:(hadoop,1) ==> (hadoop,<1>)
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
