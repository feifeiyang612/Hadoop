package com.yf.bigdata.kafka.consumer;

import com.yf.bigdata.kafka.model.DemoObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.List;

/**
 * @Author: YangFei
 * @Description: 消息消费者（group1）
 * @create: 2019-12-22 21:49
 */
@Component("groupListener1")
public class GroupListener1 {
    private static final Logger logger = LoggerFactory.getLogger(GroupListener1.class);

//    @KafkaListener(topics = {"myTopic-1"}, groupId = "group1")
//    public void listenTopic(DemoObj data) {
//        System.out.println("Group1收到消息：" + data);
//        logger.info(MessageFormat.format("Group1收到消息：{0}", data));
//    }

    @KafkaListener(topics = {"myTopic"}, groupId = "group1", containerFactory = "batchContainerFactory")
    public void listenTopic1(List<String> data) {
        System.out.println("Group1收到消息：" + data);
        logger.info(MessageFormat.format("Group1收到消息：{0}", data));
    }

}
