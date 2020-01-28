package com.hadoop.hlsijx.track.mapper;

import com.hadoop.hlsijx.utils.IPParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author hlsijx
 */
public class TrackMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        context.write(new Text(fields[5]), new LongWritable(1));
    }
}
