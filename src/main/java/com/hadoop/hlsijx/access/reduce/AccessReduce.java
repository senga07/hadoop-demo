package com.hadoop.hlsijx.access.reduce;

import com.hadoop.hlsijx.access.model.Access;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author hlsijx
 */
public class AccessReduce extends Reducer<Text, Access, NullWritable, Access> {

    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {

        long up = 0;
        long down = 0;
        while (values.iterator().hasNext()){
            Access access = values.iterator().next();
            up += access.getUp();
            down += access.getDown();
        }

        //注意：如果 Reduce 输出 Key 的类型为 NullWritable，则不能使用 Combiner
        //因为 Combiner 是一个中间过程，如果 key 设置为 null 则后面就无法通过键值获取了
        context.write(NullWritable.get(), new Access(key.toString(), up, down));
    }
}
