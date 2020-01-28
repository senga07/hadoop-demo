package com.hadoop.hlsijx.demo;

import com.hadoop.hlsijx.model.FileDetail;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static com.hadoop.hlsijx.HadoopConfig.getFileSystem;

/**
 * @author hlsijx
 */
public class DemoTest {

    private FileSystem fileSystem;

    @Before
    public void before() throws Exception {
        fileSystem = getFileSystem();
    }

    @After
    public void after(){
        fileSystem = null;
    }

    /**
     * 创建空文件夹
     * shell命令：hadoop fs -mkdir [-p] <paths>
     *
     * e.g.创建一个空文件夹/hadoop
     * hadoop fs -mkdir /hadoop
     */
    @Test
    public void mkdir() throws IOException {

        //创建该文件夹的HDFS路径
        String path = defaultDirPath;

        boolean isSuccess =  fileSystem.mkdirs(new Path(path));
        System.out.println("创建文件夹" + (isSuccess ? "成功" : "失败"));
    }

    /**
     * 创建空文件并写入内容
     * shell命令：
     * 创建一个空文件：hadoop fs -touchz URI [URI ...]
     * 写入内容：hadoop fs -appendToFile <localsrc> ... <dst>
     *
     * e.g.创建空文件/hadoop/001.txt并写入
     * 创建文件/hadoop/001.txt：hdfs dfs -touchz /hadoop/001.txt
     * 写入"hello hadoop"：echo "hello hadoop" | hdfs dfs -appendToFile - /hadoop/001.txt
     */
    @Test
    public void createFile(){

        //创建该文件的HDFS路径
        String path = defaultFilePath;

        //写入文件内容
        String text = "hello hadoop";

        try (FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(path))){
            fsDataOutputStream.writeUTF(text);
            fsDataOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读文件
     * shell命令:hadoop fs -text <src> 或 hadoop fs -cat [-ignoreCrc] URI [URI ...]
     *
     * e.g.读取文件/hadoop/001.txt
     * hdfs dfs -text /hadoop/001.txt 或 hdfs dfs -cat /hadoop/001.txt
     */
    @Test
    public void readFile() throws IOException{

        //读取该文件夹的HDFS路径
        String path = defaultFilePath;

        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(path));
        IOUtils.copyBytes(fsDataInputStream, System.out, 1024);
    }

    /**
     * 重命名文件
     * shell命令：hadoop fs -mv URI [URI ...] <dest>
     *
     * e.g.将文件/hadoop/001.txt重命名为/hadoop/002.txt
     * hadoop fs -mv /hadoop/001.txt /hadoop/002.txt
     */
    @Test
    public void rename() throws IOException{

        //原文件名称
        String oldName = defaultFilePath;

        //文件新名称
        String newName = defaultDirPath + "/002.txt";

        boolean isSuccess = fileSystem.rename(new Path(oldName), new Path(newName));
        System.out.println("重命名文件" + (isSuccess ? "成功" : "失败"));
    }

    /**
     * 上传本地文件到HDSF
     * shell命令：hadoop fs -copyFromLocal <localsrc> URI 或
     * hadoop fs -put [-f] [-p] [-l] [-d] [ - | <localsrc1> .. ]. <dst>
     *
     * e.g.上传本地文件/etc/hosts到HDFS的/hadoop文件夹下
     * hadoop fs -copyFromLocal /etc/hosts /hadoop/hosts 或
     * hadoop fs -put /etc/hosts /hadoop/hosts
     */
    @Test
    public void copyFromLocalFile() throws IOException{

        //本地文件路径
        String src = "/etc/hosts";

        //HDFS文件路径
        String dst = defaultDirPath;

        fileSystem.copyFromLocalFile(new Path(src), new Path(dst));
    }

    /**
     * 带进度条的文件上传
     */
    @Test
    public void copyFromLocalFileWithProgress() throws IOException{

        //本地待上传文件路径
        String src = "/etc/hosts";

        //HDFS文件路径
        String dst = defaultDirPath;

        InputStream inputStream = new BufferedInputStream(new FileInputStream(new File(src)));
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(dst), () -> System.out.print("."));
        IOUtils.copyBytes(inputStream, fsDataOutputStream, 4096);
    }

    /**
     * 下载HDFS文件到本地
     * shell命令：hadoop fs -copyToLocal [-ignorecrc] [-crc] URI <localdst> 或
     * hadoop fs -get [-ignorecrc] [-crc] [-p] [-f] <src> <localdst>
     *
     * e.g.下载/hadoop/hosts到本地/usr/tmp
     * hadoop fs -copyToLocal /hadoop/hosts /usr/tmp 或
     * hadoop fs -get /hadoop/hosts /usr/tmp
     */
    @Test
    public void copyToLocalFile() throws IOException {

        //HDFS文件路径
        String src = defaultDirPath + "/hosts";

        //本地文件路径
        String dst = "/usr/tmp";

        fileSystem.copyToLocalFile(false, new Path(src), new Path(dst), true);
    }

    /**
     * 展示文件列表
     * shell命令：hadoop fs -ls [-C] [-d] [-h] [-q] [-R] [-t] [-S] [-r] [-u] [-e] <args>
     *
     * e.g.展示根目录/下的文件
     * hadoop fs -ls /
     */
    @Test
    public void listFile() throws IOException{

        //HDFS文件路径
        String path = defaultDirPath;

        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));

        for (FileStatus fileStatus : fileStatuses){
            System.out.println(coverToFileDetail(fileStatus).toString());
        }
    }

    /**
     * 递归列出文件夹下的所有文件
     *
     * e.g.展示根目录/下的所有文件
     * hdfs dfs -ls -R / 或 hdfs dfs -ldr /
     */
    @Test
    public void listFileByRecursion() throws IOException{

        //HDFS文件路径
        String path = defaultDirPath;

        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(path), true);

        while (iterator.hasNext()) {
            System.out.println(coverToFileDetail(iterator.next()).toString());
        }
    }

    /**
     * 获取文件块信息
     */
    @Test
    public void getFileBlockLocations() throws IOException{

        //HDFS文件路径
        String path = defaultDirPath + "/002.txt";

        FileStatus fileStatus = fileSystem.getFileStatus(new Path(path));

        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0 , fileStatus.getLen());

        for (BlockLocation blockLocation : blockLocations) {
            String[] cachedHosts = blockLocation.getNames();
            for (String cachedHost : cachedHosts) {
                System.out.println(cachedHost);
            }
        }

    }

    /**
     * 递归删除文件
     * shell命令：
     * hadoop fs -rm [-f] [-r |-R] [-skipTrash] [-safely] URI [URI ...]
     */
    @Test
    public void deleteFileByRecursion() throws IOException{

        //HDFS待删除文件路径
        String path = defaultDirPath;

        boolean isSuccess = fileSystem.delete(new Path(path), true);
        System.out.println("删除文件" + (isSuccess ? "成功" : "失败"));
    }

    private FileDetail coverToFileDetail(FileStatus fileStatus){
        FileDetail fileDetail = new FileDetail();
        fileDetail.setFileType(fileStatus.isDirectory() ? "文件夹" : "文件");
        fileDetail.setPermission(fileStatus.getPermission() + "");
        fileDetail.setOwner(fileStatus.getOwner());
        fileDetail.setGroup(fileStatus.getGroup());
        fileDetail.setReplication(fileStatus.getReplication() + "");
        fileDetail.setPath(fileStatus.getPath() + "");
        return fileDetail;
    }

    private String defaultDirPath = "/hadoop";
    private String defaultFilePath = defaultDirPath + "/001.txt";
}
