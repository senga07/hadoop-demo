package com.hadoop.hlsijx.reducejoin.driver;

import com.hadoop.hlsijx.reducejoin.mapper.ReduceJoinMapper;
import com.hadoop.hlsijx.reducejoin.model.JoinData;
import com.hadoop.hlsijx.reducejoin.reduce.ReduceJoinReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author hlsijx
 */
public class ReduceJoinDriverLocal {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:/application/hadoop-2.6.0-cdh5.15.1");
        System.setProperty("HADOOP_USER_NAME", "root");

        String inputPathEmp = "./txt/empdept/emp.txt";
        String inputPathDept = "./txt/empdept/dept.txt";
        String outputPath = "./txt/empdept/output";

        //创建job
        Job job = Job.getInstance();

        //设置job运行的主类
        job.setJarByClass(ReduceJoinDriverLocal.class);

        //设置运行的mapper类
        job.setMapperClass(ReduceJoinMapper.class);

        //设置运行的reducer类
        job.setReducerClass(ReduceJoinReduce.class);

        //设置mapper(key,value)输出类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(JoinData.class);

        //设置reducer(key,value)输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path outputDir = new Path(outputPath);
        outputDir.getFileSystem(new Configuration()).delete(outputDir, true);

        //设置文件输入输出路径
        MultipleInputs.addInputPath(job, new Path(inputPathDept), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(inputPathEmp), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.waitForCompletion(true);
    }
}
