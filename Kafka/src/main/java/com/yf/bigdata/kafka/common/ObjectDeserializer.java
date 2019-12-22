package com.yf.bigdata.kafka.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.util.SerializationUtils;

import java.util.Map;

/**
 * @Author: YangFei
 * @Description: 需要注意的是，改变消息编码器和解码器之后需要清空Topic中原有消息或者使用新的Topic，
 * 否则原来的字符串消息在反序列化时会出现异常，切记!!!
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
