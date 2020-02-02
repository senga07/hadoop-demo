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

import static com.hadoop.hlsijx.HadoopConfig.getConfiguration;

/**
 * @author hlsijx
 * 特点：join在reduce端完成
 * 使用场景：两张大表
 * 实现原理：
 *      在mapper时，对同一个key的数据来源做标记；
 *      在reduce时，对同一个key的不同数据来源的数据做笛卡尔集
 */
public class ReduceJoinDriver {

    public static void main(String[] args) throws Exception {

        String inputEmpPath = args[0];
        String inputDeptPath = args[1];
        String outputPath = args[2];

        //创建job
        Job job = Job.getInstance(getConfiguration());

        //设置job运行的主类
        job.setJarByClass(ReduceJoinDriver.class);

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
        outputDir.getFileSystem(getConfiguration()).delete(outputDir, true);

        //检查文件是否已存在，若存在，则递归删除
        Path output = new Path(outputPath);
        output.getFileSystem(getConfiguration()).delete(output, true);

        //设置文件输入输出路径
        MultipleInputs.addInputPath(job, new Path(inputEmpPath), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(inputDeptPath), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.waitForCompletion(true);
    }
}
