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

    //id：消费者的id，当GroupId没有被配置的时候，默认id为GroupId
//    @KafkaListener(id = "test", topics = {"myTopic"})
//    public void listen(String data) {
//        System.out.println("SimpleConsumer收到消息：" + data);
//        logger.info(MessageFormat.format("SimpleConsumer收到消息：{0}", data));
//    }

}