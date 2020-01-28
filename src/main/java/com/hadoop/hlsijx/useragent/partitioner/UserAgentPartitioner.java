package com.hadoop.hlsijx.useragent.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author hlsijx
 */
public class UserAgentPartitioner extends Partitioner<Text, LongWritable> {

    @Override
    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
        if (text.toString().contains("Windows")){
            return 0;
        } else if (text.toString().contains("iPhone")){
            return 1;
        } else if (text.toString().contains("Android")){
            return 2;
        }
        return 3;
    }
}
