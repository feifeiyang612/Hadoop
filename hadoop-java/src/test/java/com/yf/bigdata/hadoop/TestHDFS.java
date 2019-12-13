package com.yf.bigdata.hadoop;

import com.yf.bigdata.hadoop.entity.User;
import com.yf.bigdata.hadoop.utils.HDFSIO;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

/**
 * @Author: YangFei
 * @Description: 测试HDFS的基本操作
 * @create: 2019-12-13 10:40
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@WebAppConfiguration
public class TestHDFS {

    @Autowired
    private HDFSIO hdfsio;

    /**
     * 测试创建HDFS目录
     */
    @Test
    public void testMkdir() {
        boolean result1 = hdfsio.mkdir("/testDir");
        System.out.println("创建结果：" + result1);

        boolean result2 = hdfsio.mkdir("/testDir/subDir");
        System.out.println("创建结果：" + result2);
    }

    /**
     * 测试上传文件
     */
    @Test
    public void testUploadFile() {
        //测试上传三个文件
        hdfsio.uploadFileToHdfs("C:/Users/yangfei/Desktop/a.txt", "/testDir");
        hdfsio.uploadFileToHdfs("C:/Users/yangfei/Desktop/b.txt", "/testDir");

        hdfsio.uploadFileToHdfs("C:/Users/yangfei/Desktop/c.txt", "/testDir/subDir");
    }

    /**
     * 测试列出某个目录下面的文件
     */
    @Test
    public void testListFiles() {
        List<Map<String, Object>> result = hdfsio.listFiles("/testDir", null);

        result.forEach(fileMap -> {
            fileMap.forEach((key, value) -> {
                System.out.println(key + "--" + value);
            });
            System.out.println();
        });
    }

    /**
     * 测试下载文件
     */
    @Test
    public void testDownloadFile() {
        hdfsio.downloadFileFromHdfs("/testDir/a.txt", "C:/Users/yangfei/Desktop/test.txt");
    }

    /**
     * 测试打开HDFS上面的文件
     */
    @Test
    public void testOpen() throws IOException {
        FSDataInputStream inputStream = hdfsio.open("/testDir/a.txt");

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        reader.close();
    }

    /**
     * 测试打开HDFS上面的文件，并转化为Java对象
     */
    @Test
    public void testOpenWithObject() throws IOException {
        User user = hdfsio.openWithObject("/testDir/b.txt", User.class);
        System.out.println(user);
    }

    /**
     * 测试重命名
     */
    @Test
    public void testRename() {
        hdfsio.rename("/testDir/b.txt", "/testDir/b_new.txt");

        //再次遍历
        testListFiles();
    }

    /**
     * 测试删除文件
     */
    @Test
    public void testDelete() {
        hdfsio.delete("/testDir/b_new.txt");

        //再次遍历
        testListFiles();
    }

    /**
     * 测试获取某个文件在HDFS集群的位置
     */
    @Test
    public void testGetFileBlockLocations() throws IOException {
        BlockLocation[] locations = hdfsio.getFileBlockLocations("/testDir/a.txt");

        if (locations != null && locations.length > 0) {
            for (BlockLocation location : locations) {
                System.out.println(location.getHosts()[0]);
            }
        }
    }
}
