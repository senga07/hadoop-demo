package com.hadoop.hlsijx.wordcount.driver;

import com.hadoop.hlsijx.wordcount.mapper.WordCountMapper;
import com.hadoop.hlsijx.wordcount.reduce.WordCountReduce;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static com.hadoop.hlsijx.HadoopConfig.getConfiguration;
import static com.hadoop.hlsijx.HadoopConfig.getFileSystem;

/**
 * @author hlsijx
 */
public class WordCountDriver {

    public static void main(String[] args) throws Exception {

        String inputPath = StringUtils.isEmpty(args[0]) ? "./txt/wordcount/input.txt" : args[0];
        String outputPath = StringUtils.isEmpty(args[1]) ? "./txt/wordcount/output" : args[1];

        //创建job，若要本地测试，不传configuration即可
        Job job = Job.getInstance(getConfiguration());

        //设置job运行的主类
        job.setJarByClass(WordCountDriver.class);

        //设置运行的mapper类
        job.setMapperClass(WordCountMapper.class);

        //设置运行的combiner类:在shuffle前先进行一次聚合，节省网络带宽，提高性能
        //局限性：求平均数（除法操作要慎重）
        job.setCombinerClass(WordCountReduce.class);

        //设置运行的reducer类
        job.setReducerClass(WordCountReduce.class);

        //设置mapper(key,value)输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reducer(key,value)输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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
