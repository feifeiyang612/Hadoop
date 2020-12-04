package com.yf.bigdata.kafka.hollysys.support;

import com.yf.bigdata.kafka.hollysys.listener.KafkaProducerMessageListener;

/**
 * @author: YangFei
 * @description: Kafka默认生产者消息监听处理
 * @create:2020-12-03 15:30
 */
public class DefaultKafkaProducerMessageListener extends KafkaProducerMessageListener {
    @Override
    public void init() {
        logger.info("--Kafka Producer default...");
    }
}