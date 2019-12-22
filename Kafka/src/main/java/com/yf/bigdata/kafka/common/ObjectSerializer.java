package com.yf.bigdata.kafka.common;

import org.apache.kafka.common.serialization.Serializer;
import org.springframework.util.SerializationUtils;

import java.util.Map;

/**
 * @Author: YangFei
 * @Description: 自定义消息编码器
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
