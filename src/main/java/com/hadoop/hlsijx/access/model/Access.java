package com.hadoop.hlsijx.access.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author hlsijx
 * 流量日志统计分析之自定义对象
 * 如何自定义对象：
 * 1、实现 Writable 接口
 * 2、重写 write 和 readFields 方法
 * 3、定义需要的字段
 */
public class Access implements Writable {

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(mobile);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        //注意：写顺序与读顺序要保持一致
        mobile = in.readUTF();
        up = in.readLong();
        down = in.readLong();
        sum = up + down;
    }

    public Access(String mobile, Long up, Long down) {
        this.mobile = mobile;
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    /**
     * 手机号
     */
    private String mobile;

    /**
     * 上行流量
     */
    private Long up;

    /**
     * 下行流量
     */
    private Long down;

    /**
     * 流量和
     */
    private Long sum;

    public Long getUp() {
        return up;
    }

    public void setUp(Long up) {
        this.up = up;
    }

    public Long getDown() {
        return down;
    }

    public Access() {
    }

    @Override
    public String toString() {
        return "Access{" +
                "mobile='" + mobile + '\'' +
                ", up=" + up +
                ", down=" + down +
                ", sum=" + sum +
                '}';
    }
}
