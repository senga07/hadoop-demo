package com.hadoop.hlsijx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

/**
 * @author hlsijx
 */
public class HadoopConfig {

    /**
     * hadoop连接地址
     */
    private static String hadoopUrl = "hdfs://47.105.57.238:8020";
    /**
     * hadoop连接用户
     */
    private static String hadoopUser = "root";

    public static Configuration getConfiguration(){

        //如果是Windows系统，需要下载hadoop到本地，并设置地址
        String localHadoopUrl = "D:/application/hadoop-2.6.0-cdh5.15.1";
        System.setProperty("hadoop.home.dir", localHadoopUrl);
        System.setProperty("HADOOP_USER_NAME", hadoopUser);

        //设置副本数，默认副本系数为3
        String replication = "1";
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", replication);
        configuration.set("fs.defaultFS", hadoopUrl);
        configuration.set("dfs.client.use.datanode.hostname", "true");

        return configuration;
    }

    public static FileSystem getFileSystem() throws Exception {

        Configuration configuration = getConfiguration();
        return FileSystem.get(new URI(hadoopUrl), configuration, hadoopUser);
    }

    HadoopConfig() {}
}
