package com.yf.bigdata.kafka.hollysys.factory;

import org.springframework.beans.factory.InitializingBean;

/**
 * @author: YangFei
 * @description: 服务生命周期
 * @create:2020-12-03 15:31
 */
public abstract class AbstractLifeCycle implements InitializingBean, LifeCycle {
    protected boolean running = true;

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
    }

    protected boolean success = false;

    public boolean isSuccess() {
        return success;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

    }
}
