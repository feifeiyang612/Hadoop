package com.yf.bigdata.kafka.consumer;

import com.yf.bigdata.kafka.producer.SimpleProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

/**
 * @Author: YangFei
 * @Description: 消息消费者的第一个示例
 * @create: 2019-12-20 23:53
 */
@Component("simpleConsumer")
public class SimpleConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

    @KafkaListener(id = "test", topics = {"topic-test"})
    public void listen(String data) {
        System.out.println("SimpleConsumer收到消息：" + data);
        logger.info(MessageFormat.format("SimpleConsumer收到消息：{0}", data));
    }

}