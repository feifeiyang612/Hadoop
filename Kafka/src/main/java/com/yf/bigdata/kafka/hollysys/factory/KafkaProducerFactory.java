package com.yf.bigdata.kafka.hollysys.factory;

import com.yf.bigdata.kafka.hollysys.listener.KafkaProducerMessageListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;

import java.util.Map;
import java.util.concurrent.Executors;

/**
 * @author: YangFei
 * @description:
 * @create:2020-12-03 15:38
 */
public class KafkaProducerFactory extends AbstractKafkaFactory {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerFactory.class);
    private String acks;
    private int retries = 2147483647;
    private int batchSize = 1048576;//1*1024*1024=1M;
    private int linger = 100;
    private int requestSize = 1048576;//1*1024*1024=1M;
    private long maxBlock = 9223372036854775807L;
    private int memory = 33554432;//32*1024*1024=32M
    private boolean autoFlush = false;
    private KafkaProducerMessageListener listener;
    private DefaultKafkaProducerFactory<String, String> factory;
    private ProducerListener<String, String> callback;

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setRequestSize(int requestSize) {
        this.requestSize = requestSize;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public void setLinger(int linger) {
        this.linger = linger;
    }

    public void setMaxBlock(long maxBlock) {
        this.maxBlock = maxBlock;
    }

    public void setMemory(int memory) {
        this.memory = memory;
    }

    public void setAutoFlush(boolean autoFlush) {
        this.autoFlush = autoFlush;
    }

    public DefaultKafkaProducerFactory<String, String> getFactory() {
        return factory;
    }

    public void setFactory(DefaultKafkaProducerFactory<String, String> factory) {
        this.factory = factory;
    }

    public KafkaProducerMessageListener getListener() {
        return listener;
    }

    public ProducerListener<String, String> getCallback() {
        return callback;
    }

    public void setListener(KafkaProducerMessageListener listener) {
        this.listener = listener;
    }

    public void setCallback(ProducerListener<String, String> callback) {
        this.callback = callback;
    }

    /**
     * @decription 初始化配置
     * @author yi.zhang
     * @time 2017年6月2日 下午2:15:57
     */
    private void init() {
        try {
            Map<String, Object> config = deafultConfig();
            config.put(ProducerConfig.ACKS_CONFIG, acks);
            config.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
            config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, requestSize);
            // 默认立即发送，这里这是延时毫秒数
            config.put(ProducerConfig.LINGER_MS_CONFIG, linger);
            config.put(ProducerConfig.RETRIES_CONFIG, retries);
            config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlock);
            // 生产者缓冲大小，当缓冲区耗尽后，额外的发送调用将被阻塞。时间超过max.block.ms将抛出TimeoutException
            config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, memory);

            if (StringUtils.isNotBlank(encoding)) {
                // 编码
                config.put("serializer.encoding", encoding);
            }
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            // 创建kafka的生产者类
            factory = new DefaultKafkaProducerFactory<>(config);
            if (callback == null) {
                callback = new ProducerListener<String, String>() {
                    @Override
                    public void onSuccess(String topic, Integer partition, String key, String value, RecordMetadata recordMetadata) {
                        if (false || logger.isDebugEnabled()) {
                            logger.info("--[Kafka producer({})] send success,data:{}", topic, value);
                        }
                    }

                    @Override
                    public void onError(String topic, Integer partition, String key, String value, Exception e) {
                        logger.error("--[Kafka producer(" + topic + ")] send error,data:" + value + "!", e);
                    }

                    @Override
                    public boolean isInterestedInSuccess() {
                        return true;
                    }
                };
            }
            KafkaTemplate<String, String> template = new KafkaTemplate<>(factory, autoFlush);
            template.setProducerListener(callback);
            String defaultTopic = KAFKA_DEFAULT_TOPIC;
            if (topics != null && topics.length > 0) {
                defaultTopic = topics[0];
            }
            template.setDefaultTopic(defaultTopic);
            listener.setTemplate(template);
            success = true;
            logger.info("--Kafka Producer Config({}) init success...", servers);
        } catch (Exception e) {
            success = false;
            logger.error("-----Kafka Producer Config init Error-----", e);
        }
    }

    @Override
    public void start() {
        init();
        Executors.newSingleThreadExecutor().execute(() -> {
            while (running) {
                try {
//                    if (factory == null || !success || !ping(servers)) {
//                        reconnect();
//                    }
                    Thread.sleep(1000l);
                } catch (InterruptedException e) {
                    logger.error("--Kafka Interrupted...", e);
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    logger.error("--Kafka Exception...", e);
                }
            }
        });
    }

    @Override
    public boolean delay() {
        return false;
    }

    @Override
    public void reconnect() {
        close();
        init();
    }

    /**
     * 关闭服务
     */
    @Override
    public void close() {
        if (factory != null) {
            try {
                factory.destroy();
            } catch (Exception e) {
                logger.error("-----Kafka close Error-----", e);
            }
        }
    }
}
