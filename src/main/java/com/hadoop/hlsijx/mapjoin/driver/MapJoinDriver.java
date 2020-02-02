package com.hadoop.hlsijx.mapjoin.driver;

import com.hadoop.hlsijx.mapjoin.mapper.MapJoinMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

import static com.hadoop.hlsijx.HadoopConfig.getConfiguration;

/**
 * @author hlsijx
 * 特点：join在map端完成，没有shulle的过程
 * 使用场景：一张大表和一张小表
 * 实现原理：
 *      将小表的数据放入缓存中，大表取缓存数据进行笛卡尔集
 */
public class MapJoinDriver {

    public static void main(String[] args) throws Exception {

        String inputEmpPath = args[0];
        String inputDeptPath = args[1];
        String outputPath = args[2];

        //创建job
        Job job = Job.getInstance(getConfiguration());

        //设置job运行的主类
        job.setJarByClass(MapJoinDriver.class);

        //设置运行的mapper类
        job.setMapperClass(MapJoinMapper.class);

        //设置mapper(key,value)输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //将小文件加入分布式缓存中
        job.addCacheFile(new URI(inputDeptPath));
        //关闭reduce
        job.setNumReduceTasks(0);

        //检查文件是否已存在，若存在，则递归删除
        Path outputDir = new Path(outputPath);
        outputDir.getFileSystem(getConfiguration()).delete(outputDir, true);

        //设置文件输入输出路径
        FileInputFormat.setInputPaths(job, new Path(inputEmpPath));
        FileOutputFormat.setOutputPath(job, outputDir);

        job.waitForCompletion(true);
    }
}
