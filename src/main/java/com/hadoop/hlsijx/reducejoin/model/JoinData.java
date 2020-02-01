package com.hadoop.hlsijx.reducejoin.model;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinData implements Writable {

    private String data;
    private Integer isemp;

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(isemp);
        out.writeUTF(data);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        isemp = in.readInt();
        data = in.readUTF();
    }

    public Integer getIsemp() {
        return isemp;
    }

    public void setIsemp(Integer isemp) {
        this.isemp = isemp;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public JoinData(String data, Integer isemp) {
        this.data = data;
        this.isemp = isemp;
    }

    public JoinData() {
    }
}
