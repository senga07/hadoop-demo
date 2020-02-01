package com.hadoop.hlsijx.reducejoin.reduce;

import com.hadoop.hlsijx.reducejoin.model.JoinData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hlsijx
 * 1、根据数据来源标志将数据分类
 * 2、将两个list做笛卡尔积
 */
public class ReduceJoinReduce extends Reducer<LongWritable, JoinData, Text, NullWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<JoinData> values, Context context) throws IOException, InterruptedException {

        //将员工数据与部门数据分类
        List<String> empList = new ArrayList<>();
        List<String> deptList = new ArrayList<>();
        for (JoinData value : values) {
            if (value.getIsemp() == 1){
                empList.add(value.getData());
            } else {
                deptList.add(value.getData());
            }
        }

        //做笛卡尔积
        for (String dept : deptList) {
            for (String emp : empList) {
                context.write(new Text(emp + "\t" + dept), NullWritable.get());
            }
        }
    }
}
