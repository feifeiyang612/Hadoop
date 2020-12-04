package com.yf.bigdata.kafka.hollysys;

import com.alibaba.fastjson.JSONObject;
import com.yf.bigdata.kafka.hollysys.listener.KafkaProducerMessageListener;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author: YangFei
 * @description:
 * @create:2020-12-03 16:06
 */
@Log4j2
@Component
@Order(1)
public class KafkaProducerExecutor implements ApplicationRunner {

    private static final String TOPIC = "HOLLYSYS_KAFKA";
    @Autowired
    private KafkaProducerMessageListener producer;
    public static ScheduledExecutorService schedule = Executors.newScheduledThreadPool(4);

    @Override
    public void run(ApplicationArguments args) throws Exception {
        schedule.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                String data = "[" + DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss") + "]IN/OUT消息";
                JSONObject obj = new JSONObject();
                obj.put("content", data);
                obj.put("name", "" + new Date().getTime() % 10);
                producer.send(TOPIC, obj.toJSONString());
                log.info("topic:" + TOPIC + "=====" + obj.toJSONString());
                log.info("==========================================Send Success=======================================");
            }
        }, 1, 10, TimeUnit.SECONDS);
    }
}
