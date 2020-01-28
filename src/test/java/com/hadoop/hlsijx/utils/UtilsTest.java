package com.hadoop.hlsijx.utils;

import org.junit.Test;

import static com.hadoop.hlsijx.etl.mapper.ETLMapper.getPageId;

/**
 * @author hlsijx
 * 工具测试类
 */
public class UtilsTest {

    @Test
    public void IPParserTest(){
        IPParser ipParser = IPParser.getInstance();
        IPParser.RegionInfo regionInfo = ipParser.analyseIp("112.10.194.75");
        System.out.println(regionInfo.getCountry());
        System.out.println(regionInfo.getProvince());
        System.out.println(regionInfo.getCity());
    }

    @Test
    public void getFiledTest(){
        String value = "20946835322\u0001http://www.yihaodian.com/1/?tracker_u=2225501&type=3\u0001http://www.baidu.com/s?wd=1%E5%8F%B7%E5%BA%97&rsv_bp=0&ch=&tn=baidu&bar=&rsv_spt=3&ie=utf-8&rsv_sug3=5&rsv_sug=0&rsv_sug1=4&rsv_sug4=313&inputT=4235\u00011号店\u00011\u0001SKAPHD3JZYH9EE9ACB1NGA9VDQHNJMX1NY9T\u0001\u0001\u0001\u0001\u0001PPG4SWG71358HGRJGQHQQBXY9GF96CVU\u00012225501\u0001\\N\u0001124.79.172.232\u0001\u0001msessionid:YR9H5YU7RZ8Y94EBJNZ2P5W8DT37Q9JH,unionKey:2225501\u0001\u00012013-07-21 09:30:01\u0001\\N\u0001http://www.baidu.com/s?wd=1%E5%8F%B7%E5%BA%97&rsv_bp=0&ch=&tn=baidu&bar=&rsv_spt=3&ie=utf-8&rsv_sug3=5&rsv_sug=0&rsv_sug1=4&rsv_sug4=313&inputT=4235\u00011\u0001\u0001\\N\u0001null\u0001-10\u0001\u0001\u0001\u0001\u0001Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; MATP; Media Center PC 6.0; .NET4.0C; InfoPath.2; .NET4.0E)\u0001Win32\u0001\u0001\u0001\u0001\u0001\u0001上海市\u00011\u0001\u00012013-07-21 09:30:01\u0001上海市\u0001\u000166\u0001\u0001\u0001\u0001\u0001\\N\u0001\\N\u0001\\N\u0001\\N\u00012013-07-21";
        String[] fields = value.split("\001");
        String url = fields[1];
        System.out.println("url:" + url);
        String referer = fields[2];
        System.out.println("referer:" + referer);
        String ip = fields[13];
        System.out.println("ip:" + ip);
        String time = fields[17];
        System.out.println("time:" + time);
        String userAgent = fields[29];
        System.out.println("userAgent:" + userAgent);
    }

    @Test
    public void getPageIdTest(){
        String pageid = getPageId("http://www.yihaodian.com/cms/view.do?topicId=19004");
        System.out.println(pageid);
    }
}
