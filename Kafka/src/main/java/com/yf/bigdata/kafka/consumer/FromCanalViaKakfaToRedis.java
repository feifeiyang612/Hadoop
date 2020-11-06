package com.yf.bigdata.kafka.consumer;

import com.yf.bigdata.kafka.common.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.List;

/**
 * @author: YangFei
 * @description:
 * @create:2020-11-06 17:02
 */

@Component
public class FromCanalViaKakfaToRedis {

    @Autowired
    RedisUtil redisUtil;
    private static final Logger logger = LoggerFactory.getLogger(GroupListener2.class);

    @KafkaListener(topics = {"canaltopic"})
    public void listenCanalTopic(List<String> data) {
        System.out.println("Group1收到消息：" + data);
        redisUtil.set("Canal_Kafka_Redis", data);
        logger.info(MessageFormat.format("Canal收到消息：{0}", data));
    }

}
