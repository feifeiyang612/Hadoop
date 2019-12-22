package com.yf.bigdata.kafka.consumer;

import com.yf.bigdata.kafka.model.DemoObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

/**
 * @Author: YangFei
 * @Description: 多消费者组消费同一条消息——消息消费者（group2）
 * @create: 2019-12-22 21:51
 */
@Component("groupListener2")
public class GroupListener2 {
    private static final Logger logger = LoggerFactory.getLogger(GroupListener2.class);

	@KafkaListener(topics={"myTopic"},groupId="group2")
	public void listenTopic2(DemoObj data){
		System.out.println("Group2收到消息：" + data);
		logger.info(MessageFormat.format("Group2收到消息：{0}", data));
	}

//	@KafkaListener(topics={"topic-test2"},groupId="group2")
//	public void listenTopic2_2(DemoObj data){
//		System.out.println("Group2_2收到消息：" + data);
//		logger.info(MessageFormat.format("Group2_2收到消息：{0}", data));
//	}
}
