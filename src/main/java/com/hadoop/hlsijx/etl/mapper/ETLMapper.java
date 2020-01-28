package com.hadoop.hlsijx.etl.mapper;

import com.hadoop.hlsijx.utils.IPParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.*;

/**
 * @author hlsijx
 */
public class ETLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private IPParser ipParser;

    @Override
    protected void setup(Context context) {
        ipParser = IPParser.getInstance();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\001");
        String url = fields[1];
        String referer = fields[2];
        String ip = fields[13];
        String time = fields[17];
        String userAgent = fields[29];

        IPParser.RegionInfo regionInfo = ipParser.analyseIp(ip);
        String province = StringUtils.isEmpty(regionInfo.getProvince()) ?
                "-" : regionInfo.getProvince();

        String stringBuilder = time + "\t" +
                                url + "\t" +
                                referer + "\t" +
                                ip + "\t" +
                                userAgent + "\t" +
                                province + "\t" +
                                getPageId(url);
        context.write(NullWritable.get(), new Text(stringBuilder));
    }

    public static String getPageId(String url){
        Pattern pattern = compile("topicId=[0-9]*");
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()){
            return matcher.group().split("topicId=")[1];
        }
        return "-";
    }
}
