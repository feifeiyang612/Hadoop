package com.yf.bigdata.kafka.hollysys.factory;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Map;

/**
 * @author: YangFei
 * @description: Kafka配置工厂
 * @create:2020-12-03 15:31
 */
public abstract class AbstractKafkaFactory extends AbstractLifeCycle {
    /**
     * Kafka默认主题
     */
    public static final String KAFKA_DEFAULT_TOPIC = "KAFKA_DEFAULT_TOPIC";
    protected String servers;
    protected int timeout = 30 * 1000;
    protected String[] topics;
    private Map<String, Object> config;
    protected String encoding = "UTF8";

    public void setServers(String servers) {
        this.servers = servers;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void setTopics(String topics) {
        this.topics = StringUtils.isBlank(topics) ? new String[]{KAFKA_DEFAULT_TOPIC} : topics.split(",");
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    protected Map<String, Object> deafultConfig() {
        Map<String, Object> dconfig = Maps.newConcurrentMap();
        if (config != null) {
            dconfig.putAll(config);
        }
        config = dconfig;
        if (StringUtils.isNotBlank(servers)) {
            config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, servers);
        }
        if (!config.containsKey(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG)) {
            // 重试连接时长
            config.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, 1000L);
        }
        if (!config.containsKey(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG)) {
            // 重试连接最大时长
            config.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10 * 1000L);
        }
        if (!config.containsKey(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG)) {
            // 失败重试次数
            config.put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10 * 60 * 1000L);
        }
        if (timeout > -1) {
            // 请求超时时长
            config.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, timeout);
        }
        return config;
    }
}
