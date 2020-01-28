package com.hadoop.hlsijx.access.partitioner;

import com.hadoop.hlsijx.access.model.Access;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author hlsijx
 * 流量日志统计分析之自定义分区规则
 * 默认分区规则为 HashPartitioner：(key.hashCode() & Integer.MAX_VALUE) % numReduceTasks
 * 由此可见，Partitioner 与 ReduceTask 的个数有关
 * 因此在 Driver 类设置的时候不仅要设置 PartitionerClass，还要设置 numReduceTasks
 * 且要与 Partitioner 类中设置的分区数要保持一致
 */
public class AccessPartitioner extends Partitioner<Text, Access> {

    /**
     * 根据手机号前两位进行分区，一共分3个区：
     * 13*********
     * 15*********
     * 其他
     * @param text 手机号
     * @param access 流量统计数据
     * @param numPartitions 分区个数
     * @return 根据分区规则返回对应的分区数
     */
    @Override
    public int getPartition(Text text, Access access, int numPartitions) {

        String phone = text.toString();
        if (phone.startsWith("13")){
            return 1;
        } else if (phone.startsWith("15")){
            return 2;
        }
        return 0;
    }
}
