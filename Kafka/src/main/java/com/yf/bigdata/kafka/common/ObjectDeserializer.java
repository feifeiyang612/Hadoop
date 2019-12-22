package com.yf.bigdata.kafka.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.util.SerializationUtils;

import java.util.Map;

/**
 * @Author: YangFei
 * @Description:
 * @create: 2019-12-22 18:03
 */
public class ObjectDeserializer implements Deserializer<Object> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    /**
     * 反序列化
     */
    @Override
    public Object deserialize(String topic, byte[] data) {
        return SerializationUtils.deserialize(data);
    }

    @Override
    public void close() {

    }
}
