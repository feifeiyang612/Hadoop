package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;

/**
 * @Author: YangFei
 * @Description: HDFS相关的基本操作
 * @create: 2019-12-12 15:53
 */
public class HDFSIO {

    private Logger logger = LoggerFactory.getLogger(HDFSIO.class);
    private static final String HDFS_PATH = "hdfs://server1:9000";
    private static final String HDFS_USER = "master";
    private Configuration conf = null;
    /**
     * 默认的HDFS路径，比如：hdfs://172.21.32.80:9000
     */
    private String defaultHdfsUri;

    public HDFSIO(Configuration conf, String defaultHdfsUri) {
        this.conf = conf;
        this.defaultHdfsUri = defaultHdfsUri;
    }

    private static FileSystem fileSystem;

    /**
     * 获取HDFS文件系统
     *
     * @return org.apache.hadoop.fs.FileSystem
     */
    private FileSystem getFileSystem() throws IOException {
        try {
            fileSystem = FileSystem.get(new URI(HDFS_PATH), conf, HDFS_USER);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return fileSystem;

    }

    /**
     * @return boolean 是否创建成功
     * @Author yangfei
     * @Description 创建HDFS目录
     * @Date 16:03 2019/12/12
     * @Param path HDFS的相对目录路径，比如：/testDir
     */
    public boolean mkdir(String path) {
        //如果目录已经存在，则直接返回
        if (checkExists(path)) {
            return true;
        } else {
            FileSystem fileSystem = null;

            try {
                fileSystem = getFileSystem();

                //最终的HDFS文件目录
                String hdfsPath = generateHdfsPath(path);
                //创建目录
                return fileSystem.mkdirs(new Path(hdfsPath));
            } catch (IOException e) {
                logger.error(MessageFormat.format("创建HDFS目录失败，path:{0}", path), e);
                return false;
            } finally {
                close(fileSystem);
            }
        }
    }

    /**
     * 判断文件或者目录是否在HDFS上面存在
     *
     * @param path HDFS的相对目录路径，比如：/testDir、/testDir/a.txt
     * @return boolean
     * @author zifangsky
     * @date 2018/7/20 15:37
     * @since 1.0.0
     */
    public boolean checkExists(String path) {
        FileSystem fileSystem = null;
        try {
            fileSystem = getFileSystem();

            //最终的HDFS文件目录
            String hdfsPath = generateHdfsPath(path);

            //创建目录
            return fileSystem.exists(new Path(hdfsPath));
        } catch (IOException e) {
            logger.error(MessageFormat.format("'判断文件或者目录是否在HDFS上面存在'失败，path:{0}", path), e);
            return false;
        } finally {
            close(fileSystem);
        }
    }

    /**
     * 将相对路径转化为HDFS文件路径
     *
     * @param dstPath 相对路径，比如：/data
     * @return java.lang.String
     * @author zifangsky
     * @date 2018/7/20 15:18
     * @since 1.0.0
     */
    private String generateHdfsPath(String dstPath) {
        String hdfsPath = defaultHdfsUri;
        if (dstPath.startsWith("/")) {
            hdfsPath += dstPath;
        } else {
            hdfsPath = hdfsPath + "/" + dstPath;
        }

        return hdfsPath;
    }

    /**
     * close方法
     */
    private void close(FileSystem fileSystem) {
        if (fileSystem != null) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

}
