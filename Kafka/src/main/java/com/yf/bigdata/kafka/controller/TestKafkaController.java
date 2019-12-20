package com.yf.bigdata.kafka.controller;

import com.yf.bigdata.kafka.producer.SimpleProducer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @Author: YangFei
 * @Description:
 * @create: 2019-12-20 23:52
 */
@RestController
@RequestMapping("/kafka")
public class TestKafkaController {
    @Resource(name = "simpleProducer")
    private SimpleProducer producer;

    private final String TOPIC = "topic-test"; //测试使用topic

    @RequestMapping("/send")
    public String send(String data) {
        producer.sendMessage(TOPIC, data);

        return "发送数据【" + data + "】成功！";
    }

}
