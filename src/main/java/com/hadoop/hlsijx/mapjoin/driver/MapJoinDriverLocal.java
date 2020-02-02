package com.hadoop.hlsijx.mapjoin.driver;

import com.hadoop.hlsijx.mapjoin.mapper.MapJoinMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @author hlsijx
 */
public class MapJoinDriverLocal {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:/application/hadoop-2.6.0-cdh5.15.1");
        System.setProperty("HADOOP_USER_NAME", "root");

        String inputPathEmp = "./txt/empdept/emp.txt";
        String inputPathDept = "./txt/empdept/dept.txt";
        String outputPath = "./txt/deptemp/output";

        //创建job
        Job job = Job.getInstance();

        //设置job运行的主类
        job.setJarByClass(MapJoinDriverLocal.class);

        //设置运行的mapper类
        job.setMapperClass(MapJoinMapper.class);

        //设置mapper(key,value)输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //将小文件加入缓存中
        job.addCacheFile(new URI(inputPathDept));
        //关闭reduce
        job.setNumReduceTasks(0);

        //设置文件输入输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPathEmp));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
