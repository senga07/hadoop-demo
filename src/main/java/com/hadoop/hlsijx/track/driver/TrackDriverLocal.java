package com.hadoop.hlsijx.track.driver;

import com.hadoop.hlsijx.track.mapper.TrackMapper;
import com.hadoop.hlsijx.track.reduce.TrackReduce;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author hlsijx
 */
public class TrackDriverLocal {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:/application/hadoop-2.6.0-cdh5.15.1");
        System.setProperty("HADOOP_USER_NAME", "root");

        String inputPath = "./txt/track/input/part-r-00000";
        String outputPath = "./txt/track/output";

        //创建job
        Job job = Job.getInstance();

        //设置job运行的主类
        job.setJarByClass(TrackDriverLocal.class);

        //设置运行的mapper类
        job.setMapperClass(TrackMapper.class);

        //设置运行的reducer类
        job.setReducerClass(TrackReduce.class);

        //设置mapper(key,value)输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reducer(key,value)输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        //设置文件输入输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
