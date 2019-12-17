package com.yf.bigdata.hadoop.component;

import com.yf.bigdata.hadoop.utils.WordCountDataUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: YangFei
 * @Description: 自定义partitioner, 按照单词分区
 * @create: 2019-12-17 14:41
 */
public class CustomPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        return WordCountDataUtils.WORD_LIST.indexOf(text.toString());
    }
}
