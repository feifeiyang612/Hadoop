package com.yf.bigdata.kafka.hollysys;

import com.yf.bigdata.kafka.hollysys.config.HollysysKafkaConfig;
import com.yf.bigdata.kafka.hollysys.factory.KafkaConsumerFactory;
import com.yf.bigdata.kafka.hollysys.listener.KafkaConsumerMessageListener;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * @author: YangFei
 * @description:
 * @create:2020-12-03 16:21
 */
@Log4j2
@Component
@Order(5)
public class KafkaConsumerExecutor implements ApplicationRunner {

    private static final String TOPIC = "HOLLYSYS_KAFKA";

    KafkaConsumerFactory snapshotConsumerFactory;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        snapshotConsumerFactory = HollysysKafkaConfig.newKafkaConsumerFactory();
        snapshotConsumerFactory.setTopics(TOPIC);
        snapshotConsumerFactory.setPattern(true);
        snapshotConsumerFactory.setListener(new KakfaConsumer());
        snapshotConsumerFactory.start();
    }

    public class KakfaConsumer implements KafkaConsumerMessageListener {

        @Override
        public boolean handle(String topic, String data) {
            log.info("消息Topic：" + topic);
            log.info("消息data：" + data);
            log.info("==========================================Consumer Success=======================================");
            try {
                TimeUnit.MILLISECONDS.sleep(5);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
            return true;
        }
    }
}
