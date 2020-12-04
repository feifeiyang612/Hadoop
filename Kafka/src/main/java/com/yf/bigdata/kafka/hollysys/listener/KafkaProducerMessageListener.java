package com.yf.bigdata.kafka.hollysys.listener;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * @author: YangFei
 * @description: Kafka生产者消息监听处理
 * @create:2020-12-03 15:24
 */
public abstract class KafkaProducerMessageListener {
    protected static Logger logger = LoggerFactory.getLogger(KafkaProducerMessageListener.class);
    /**
     * Fastjson默认序列化特性
     */
    public static final SerializerFeature[] FASTJSON_DEFAULT_SERIALIZER_FEATURES = {SerializerFeature.SkipTransientField, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteNullListAsEmpty, SerializerFeature.WriteNullStringAsEmpty};

    private KafkaTemplate<String, String> template;

    public abstract void init();

    public void setTemplate(KafkaTemplate<String, String> template) {
        this.template = template;
        init();
    }

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     *
     * @param data The record contents
     */
    public boolean send(Object data) {
        return send(template.getDefaultTopic(), data);
    }

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     *
     * @param topic The topic the record will be appended to
     * @param data  The record contents
     */
    public boolean send(String topic, Object data) {
        return send(topic, data, null, null, null, null);
    }

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     *
     * @param topic     The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param data      The record contents
     */
    public boolean send(String topic, Object data, Integer partition) {
        return send(topic, data, null, partition, null, null);
    }

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     *
     * @param topic     The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param timestamp The timestamp of the record, in milliseconds since epoch. If null, the producer will assign
     *                  the timestamp using System.currentTimeMillis().
     * @param key       The key that will be included in the record
     * @param data      The record contents
     */
    public boolean send(String topic, Object data, String key, Integer partition, Long timestamp) {
        return send(topic, data, null, partition, null, null);
    }

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     *
     * @param topic     The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param timestamp The timestamp of the record, in milliseconds since epoch. If null, the producer will assign
     *                  the timestamp using System.currentTimeMillis().
     * @param key       The key that will be included in the record
     * @param data      The record contents
     * @param headers   the headers that will be included in the record
     */
    public boolean send(String topic, Object data, String key, Integer partition, Long timestamp, Iterable<Header> headers) {
        boolean flag = true;
        try {
            if (StringUtils.isBlank(topic)) {
                topic = template.getDefaultTopic();
            }
            if (StringUtils.isBlank(key)) {
                key = "DEFAULT_KAFKA_KEY";
            }
            String value = null;
            if (data != null) {
                if (data instanceof String) {
                    value = (String) data;
                } else if (data instanceof Number || data instanceof Boolean) {
                    value = data.toString();
                } else {
//                    value = JSON.toJSONString(data, SystemGlobalConfig.FASTJSON_DEFAULT_SERIALIZER_FEATURES);
                    value = JSON.toJSONString(data, FASTJSON_DEFAULT_SERIALIZER_FEATURES);
                }
            }
            template.send(new ProducerRecord<String, String>(topic, partition, timestamp, key, value, headers));
        } catch (Exception e) {
            logger.error("---kafka[" + topic + "] send error !", e);
            flag = false;
        }
        return flag;
    }
}