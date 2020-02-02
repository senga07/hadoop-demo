package com.hadoop.hlsijx.mapjoin.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author hlsijx
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<Integer, String> deptMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        //从缓存中取部门表数据，初始化部门表Map

        //HDFS上的路径，如果用该路径读文件，则报 FileNotFoundException
        String path = context.getCacheFiles()[0].toString();
        //服务器上的路径，用该路径执行成功
        String actualPath  = "/data/dept.txt";

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(actualPath))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] words = line.split("\t");
                deptMap.put(Integer.valueOf(words[0]), words[1]);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //读取员工表数据
        String[] fields = value.toString().split("\t");
        Integer deptno = Integer.valueOf(fields[7]);
        String deptname = deptMap.get(deptno);

        if (deptname != null){
            StringBuilder data = new StringBuilder();
            data.append(fields[0]).append("\t")
                    .append(fields[1]).append("\t")
                    .append(fields[5]).append("\t")
                    .append(deptname);
            context.write(new Text(data.toString()), NullWritable.get());
        }
    }
}
