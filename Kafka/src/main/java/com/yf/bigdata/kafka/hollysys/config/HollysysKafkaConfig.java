package com.yf.bigdata.kafka.hollysys.config;

import com.yf.bigdata.kafka.hollysys.factory.KafkaConsumerFactory;
import com.yf.bigdata.kafka.hollysys.factory.KafkaProducerFactory;
import com.yf.bigdata.kafka.hollysys.listener.KafkaProducerMessageListener;
import com.yf.bigdata.kafka.hollysys.support.DefaultKafkaProducerMessageListener;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author: YangFei
 * @description: Kafka配置
 * @create:2020-12-03 13:07
 */
@Configuration
public class HollysysKafkaConfig {

    private Logger logger = LoggerFactory.getLogger(HollysysKafkaConfig.class);

    private static boolean producer;
    public static boolean consumer;
    private static String servers;
    private static String topics;
    // private static boolean isPattern;
    private static int timeout;
    private static String acks;
    private static int retries;
    private static int batchSize;
    private static int linger;
    private static long maxBlock;
    private static int memory;
    private static boolean autoFlush;
    private static String groupId;
    // private static boolean isBatch;
    private static boolean autoCommit;
    private static int commitInterval;
    private static int concurrency;

    @Value("${kafka.producer.enabled:false}")
    public void setProducer(boolean enabled) {
        producer = enabled;
    }

    @Value("${kafka.consumer.enabled:false}")
    public void setConsumer(boolean enabled) {
        consumer = enabled;
    }

    @Value("${kafka.servers:}")
    public void setServers(String value) {
        servers = value;
    }

    @Value("${kafka.topics:}")
    public void setTopics(String value) {
        topics = value;
    }

    @Value("${kafka.timeout:30000}")
    public void setTimeout(int value) {
        timeout = value;
    }

    @Value("${kafka.acks:all}")
    public void setAcks(String value) {
        acks = value;
    }

    @Value("${kafka.retries:2147483647}")
    public void setRetries(int value) {
        retries = value;
    }

    @Value("${kafka.batch.size:1048576}")
    public void setBatchSize(int value) {
        batchSize = value;
    }

    @Value("${kafka.linger.ms:100}")
    public void setLinger(int value) {
        linger = value;
    }

    @Value("${kafka.max.block.ms:9223372036854775807}")
    public void setMaxBlock(long value) {
        maxBlock = value;
    }

    @Value("${kafka.buffer.memory:33554432}")
    public void setMemory(int value) {
        memory = value;
    }

    @Value("${kafka.auto.flush:false}")
    public void setAutoFlush(boolean flush) {
        autoFlush = flush;
    }

    @Value("${kafka.group.id:}")
    public void setGroupId(String value) {
        groupId = value;
    }

//    @Value("${kafka.'batch.commit:false}")
//    public void setBatch(boolean batch) {
//        isBatch = batch;
//    }
//
//    @Value("${kafka.topic.pattern:false}")
//    public void setIsPattern(boolean pattern) {
//        isPattern = pattern;
//    }

    @Value("${kafka.auto.commit:false}")
    public void setAutoCommit(boolean commit) {
        autoCommit = commit;
    }

    @Value("${kafka.auto.commit.interval.ms:1000}")
    public void setCommitInterval(int interval) {
        commitInterval = interval;
    }

    @Value("${kafka.concurrency:3}")
    public void setConcurrency(int value) {
        concurrency = value;
    }

    @Bean
    public KafkaProducerMessageListener kafkaProducerMessageListener() {
        producer = StringUtils.isNotBlank(System.getenv("KAFKA_PRODUCER_ENABLED")) ? Boolean.parseBoolean(System.getenv("KAFKA_PRODUCER_ENABLED")) : producer;
        if (!producer) {
            logger.info(">>>>>>>>>>> Kafka Producer Disabled.");
            return null;
        }
        logger.info(">>>>>>>>>>> Kafka Producer config init....");
        KafkaProducerMessageListener listener = new DefaultKafkaProducerMessageListener();
        KafkaProducerFactory factory = new KafkaProducerFactory();
        factory.setServers(servers);
        factory.setRetries(retries);
        factory.setTimeout(timeout);
        factory.setTopics(topics);
        factory.setRunning(true);
        factory.setAcks(acks);
        factory.setAutoFlush(autoFlush);
        factory.setBatchSize(batchSize);
        factory.setLinger(linger);
        factory.setMaxBlock(maxBlock);
        factory.setMemory(memory);
        factory.setListener(listener);
        factory.start();
        return listener;
    }

    /**
     * 注册一个新的消费工厂
     *
     * @return
     */
    public static KafkaConsumerFactory newKafkaConsumerFactory() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        factory.setServers(servers);
//        factory.setRetries(retries);
        factory.setTimeout(timeout);
        factory.setTopics(topics);
        factory.setRunning(true);
        factory.setGroupId(groupId);
        factory.setAutoCommit(autoCommit);
        factory.setAutoCommitInterval(commitInterval);
        factory.setConcurrency(concurrency);

        return factory;
    }

    /**
     * 注册一个新的生产工厂
     *
     * @return
     */
    public static KafkaProducerFactory newKafkaProducerFactory() {
        KafkaProducerFactory factory = new KafkaProducerFactory();
        factory.setServers(servers);
        factory.setRetries(retries);
        factory.setTimeout(timeout);
        factory.setTopics(topics);
        factory.setRunning(true);
        factory.setAcks(acks);
        factory.setAutoFlush(autoFlush);
        factory.setBatchSize(batchSize);
        factory.setLinger(linger);
        factory.setMaxBlock(maxBlock);
        factory.setMemory(memory);

        return factory;
    }
}