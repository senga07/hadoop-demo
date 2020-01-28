package com.hadoop.hlsijx.useragent.driver;

import com.hadoop.hlsijx.useragent.mapper.UserAgentMapper;
import com.hadoop.hlsijx.useragent.reduce.UserAgentReduce;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static com.hadoop.hlsijx.HadoopConfig.getConfiguration;
import static com.hadoop.hlsijx.HadoopConfig.getFileSystem;

/**
 * @author hlsijx
 * 先执行ETLDriver后，再根据ETL后的数据进行数据统计
 */
public class UserAgentDriver {

    public static void main(String[] args) throws Exception {

        String inputPath = args[0];
        String outputPath = args[1];

        //创建job
        Job job = Job.getInstance(getConfiguration());

        //设置job运行的主类
        job.setJarByClass(UserAgentDriver.class);

        //设置运行的mapper类
        job.setMapperClass(UserAgentMapper.class);

        //设置运行的reducer类
        job.setReducerClass(UserAgentReduce.class);

        //设置mapper(key,value)输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reducer(key,value)输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //检查文件是否已存在，若存在，则递归删除
        Path output = new Path(outputPath);
        FileSystem fileSystem = getFileSystem();
        if (fileSystem.exists(output)){
            fileSystem.delete(output, true);
        }

        //设置文件输入输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }
}
