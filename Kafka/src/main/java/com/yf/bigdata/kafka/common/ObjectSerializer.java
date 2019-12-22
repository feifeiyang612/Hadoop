package com.yf.bigdata.kafka.common;

import org.apache.kafka.common.serialization.Serializer;
import org.springframework.util.SerializationUtils;

import java.util.Map;

/**
 * @Author: YangFei
 * @Description: 自定义消息编码器
 * 需要注意的是，改变消息编码器和解码器之后需要清空Topic中原有消息或者使用新的Topic，
 * 否则原来的字符串消息在反序列化时会出现异常，切记!!!
 * @create: 2019-12-22 17:51
 */
public class ObjectSerializer implements Serializer<Object> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    /**
     * 序列化
     */
    @Override
    public byte[] serialize(String topic, Object data) {
        return SerializationUtils.serialize(data);
    }

    @Override
    public void close() {

    }
}
