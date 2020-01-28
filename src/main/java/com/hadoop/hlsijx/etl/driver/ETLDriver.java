package com.hadoop.hlsijx.etl.driver;

import com.hadoop.hlsijx.etl.mapper.ETLMapper;
import com.hadoop.hlsijx.track.reduce.TrackReduce;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static com.hadoop.hlsijx.HadoopConfig.getConfiguration;
import static com.hadoop.hlsijx.HadoopConfig.getFileSystem;

/**
 * @author hlsijx
 */
public class ETLDriver {

    public static void main(String[] args) throws Exception {

        String inputPath = args[0];
        String outputPath = args[1];

        //创建job
        Job job = Job.getInstance(getConfiguration());

        //设置job运行的主类
        job.setJarByClass(ETLDriver.class);

        //设置运行的mapper类
        job.setMapperClass(ETLMapper.class);

        //设置mapper(key,value)输出类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

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
