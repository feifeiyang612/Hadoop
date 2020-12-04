package com.yf.bigdata.kafka.hollysys.factory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.Maps;
import com.yf.bigdata.kafka.hollysys.listener.KafkaConsumerMessageListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

/**
 * @author: YangFei
 * @description:
 * @create:2020-12-03 15:41
 */
public class KafkaConsumerFactory extends AbstractKafkaFactory {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerFactory.class);
    public static final SerializerFeature[] FASTJSON_DEFAULT_SERIALIZER_FEATURES = {SerializerFeature.SkipTransientField, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteNullListAsEmpty, SerializerFeature.WriteNullStringAsEmpty};

    /**
     * Kafka默认消费分组
     */
    public static final String KAFKA_DEFAULT_GROUPID = "KAFKA_DEFAULT_GROUPID";
    private KafkaConsumerMessageListener listener = null;
    private ConcurrentMessageListenerContainer<String, String> service = null;
    private String groupId;
    private boolean batch = false;
    private boolean pattern = false;
    private boolean autoCommit = false;
    private int autoCommitInterval = 1000;
    private int concurrency = 3;
    private String strategy = "sticky";
    private int maxRecords = 500;
    /**
     * 是否合并处理
     */
    private boolean merge = true;

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    public void setPattern(boolean pattern) {
        this.pattern = pattern;
    }

    public void setMaxRecords(int maxRecords) {
        this.maxRecords = maxRecords;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public void setMerge(boolean merge) {
        this.merge = merge;
    }

    public void setAutoCommitInterval(int autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public void setListener(KafkaConsumerMessageListener listener) {
        this.listener = listener;
    }

    /**
     * @decription 初始化配置
     * @author yi.zhang
     * @time 2017年6月2日 下午2:15:57
     */
    private void init() {
        try {
            Map<String, Object> config = deafultConfig();
            if (StringUtils.isBlank(groupId)) {
                groupId = KAFKA_DEFAULT_GROUPID;
            }
            if (StringUtils.isNotBlank(groupId)) {
                // 消费者的组
                config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            }
            if (StringUtils.isNotBlank(encoding)) {
                // 编码
                config.put("deserializer.encoding", encoding);
            }
            if (!config.containsKey(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG)) {
                config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, timeout > 10 * 1000 ? 10 * 1000 : timeout / 3);
            }
            config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, timeout);
            config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxRecords);
            config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
            config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
            config.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, Collections.singletonList(ComsumerStrategy.strategy(strategy)));
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);//解决springboot版本2.2.10的AbstractMessageListenerContainer的161行空指针bug
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(config);
            topics = (topics == null || topics.length < 1) ? new String[]{KAFKA_DEFAULT_TOPIC} : topics;
            ContainerProperties container = pattern ? new ContainerProperties(Pattern.compile(topics[0])) : new ContainerProperties(topics);
            if (!autoCommit) {
                container.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            }
            if (batch) {
                container.setMessageListener(new BatchAcknowledgingMessageListener<String, String>() {
                    @Override
                    public void onMessage(List<ConsumerRecord<String, String>> list, Acknowledgment ack) {
                        Map<String, List<Object>> map = Maps.newConcurrentMap();
                        try {
                            if (list != null && !list.isEmpty()) {
                                for (ConsumerRecord<String, String> record : list) {
                                    String topic = record.topic();
                                    String value = record.value();
                                    String key = record.key();
                                    if (!map.containsKey(topic)) {
                                        map.put(topic, new ArrayList<Object>());
                                    }
                                    Object data = value;
                                    JSONValidator validator = JSONValidator.from(value);
                                    if (merge && validator.validate() && JSONValidator.Type.Value != validator.getType()) {
                                        if (JSONValidator.Type.Array == validator.getType()) {
                                            data = JSON.parseArray(value, JSONObject.class);
                                        } else {
                                            data = JSON.parseObject(value, JSONObject.class);
                                        }
                                    }
                                    if (false || logger.isDebugEnabled()) {
                                        logger.info("---------------Kafka Topic:{},key:{},value:{}", topic, key, data);
                                    }
                                    if (data instanceof List) {
                                        map.get(topic).addAll((List<?>) data);
                                    } else {
                                        map.get(topic).add(data);
                                    }
                                }
                            }
                            boolean success = false;
                            if (!map.isEmpty()) {
                                for (Map.Entry<String, List<Object>> obj : map.entrySet()) {
                                    String topic = obj.getKey();
                                    List<Object> values = obj.getValue();
                                    if (values != null && !values.isEmpty()) {
                                        String data = JSON.toJSONString(values, FASTJSON_DEFAULT_SERIALIZER_FEATURES);
                                        success = listener.handle(topic, data);
                                        if (!success) {
                                            break;
                                        }
                                        Thread.sleep(1);
                                    }
                                }
                            }

                            if (success && !autoCommit && ack != null) {
                                ack.acknowledge();
                            }
                            release();
                        } catch (Exception e) {
                            logger.error("--[Kafka-consumer(" + map.keySet() + ")]Data io error ! ", e);
                        }
                    }
                });
            } else {
                container.setMessageListener(new AcknowledgingMessageListener<String, String>() {
                    @Override
                    public void onMessage(ConsumerRecord<String, String> obj, Acknowledgment ack) {
                        String topic = obj.topic();
                        String value = obj.value();
                        String key = obj.key();
                        try {
                            if (false || logger.isDebugEnabled()) {
                                logger.info("---------------Kafka topic:{},key:{},value:{}", topic, key, value);
                            }
                            boolean flag = listener.handle(topic, value);
                            if (flag && !autoCommit && ack != null) {
                                ack.acknowledge();
                            }
                            release();
                        } catch (Exception e) {
                            logger.error("--[Kafka-Consumer(" + topic + ")]Data io error ! ", e);
                        }
                    }
                });
            }
            service = new ConcurrentMessageListenerContainer<>(factory, container);
            service.setConcurrency(concurrency);
            service.start();
            success = true;
            logger.info("--Kafka Consumer Config({}) init success...", servers);
        } catch (Exception e) {
            success = false;
            logger.error("-----Kafka Consumer Config init Error-----", e);
        }
    }

    @Override
    public void start() {
        boolean delay = delay();
        if (!delay) {
            init();
        }
        Executors.newSingleThreadExecutor().execute(() -> {
            while (running) {
                try {
                    if (listener == null) {
                        logger.error("-----Kafka must have at least one consumer!");
                        break;
                    }
//                    if (service == null || !success || !ping(servers) || !service.isRunning()) {
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
    public void reconnect() {
        close();
        init();
    }

    @Override
    public boolean delay() {
        return false;
    }

    /**
     * 关闭服务
     */
    @Override
    public void close() {
        if (service != null) {
            service.stop();
        }
    }

    /**
     * @author ZhangYi
     * @version 1.0.0
     * @project HSF_Common
     * @description Kafka消费策略
     * @date 2019/10/17 14:18:14
     * @Jdk 1.8
     */
    public enum ComsumerStrategy {
        RANGE,
        ROUNDROBIN,
        STICKY;

        public static Class<?> strategy(String mode) {
            if (StringUtils.isBlank(mode)) {
                return RangeAssignor.class;
            }
            if (mode.equalsIgnoreCase(RANGE.name())) {
                return RangeAssignor.class;
            }
            if (mode.equalsIgnoreCase(ROUNDROBIN.name())) {
                return RoundRobinAssignor.class;
            }
            if (mode.equalsIgnoreCase(STICKY.name())) {
                return StickyAssignor.class;
            }
            return RangeAssignor.class;
        }
    }
}