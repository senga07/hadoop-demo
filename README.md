windowns+虚拟机+centos7环境下，启动WordCountApp报空指针：

1、需要拷贝hadoop-2.6.0-cdh5.15.1文件到本地，并下载winutils.exe到hadoop-2.6.0-cdh5.15.1/bin下，下载链接： (http://social.msdn.microsoft.com/Forums/windowsazure/en-US/28a57efb-082b-424b-8d9e-731b1fe135de/please-read-if-experiencing-job-failures?forum=hdinsight)

2、在WordCountApp代码中添加一句话System.setProperty("hadoop.home.dir", "E:/hadoop-2.6.0-cdh5.15.1")，这个路径就是存放winutils.exe的父路径;

3、这个空指针问题解决后，会有一个nativeIO问题，需要添加NativeIO.java，包名为org.apache.hadoop.io.nativeio

# 目录介绍

### DemoTest
该文件下是HDFS的Java API操作以及对应的Shell

* 创建文件夹
* 创建空文件并写入



### wordcount
该目录下是一个词频统计的案例
