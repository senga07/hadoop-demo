package com.hadoop.hlsijx.track.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author hlsijx
 */
public class TrackReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long sum = 0;
        while (values.iterator().hasNext()){
            sum += values.iterator().next().get();
        }

        context.write(key, new LongWritable(sum));
    }
}
