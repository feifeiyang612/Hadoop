package com.yf.bigdata.kafka.hollysys.listener;

/**
 * @author: YangFei
 * @description: 消费者消息监听处理
 * @create:2020-12-03 15:42
 */
public interface ConsumerMessageAdapter {
    /**
     * @param topic 消息主题
     * @param data  消息数据
     * @return
     * @描述 消息消费处理
     * @author ZhangYi
     * @时间 2020年10月10日 下午1:42:14
     */
    public boolean handle(String topic, String data);
}
