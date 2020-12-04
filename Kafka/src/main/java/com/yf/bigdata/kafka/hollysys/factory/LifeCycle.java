package com.yf.bigdata.kafka.hollysys.factory;

import org.springframework.beans.factory.DisposableBean;

/**
 * @author: YangFei
 * @description: 服务生命周期
 * @create:2020-12-03 15:32
 */
public interface LifeCycle extends DisposableBean, AutoCloseable {
    /**
     * 描述: 启动服务
     *
     * @author ZhangYi
     * @date 2019-10-25 12:30:34
     */
    public void start();

    /**
     * 描述: 关闭(销毁)服务
     *
     * @author ZhangYi
     * @date 2019-10-25 12:30:34
     */
    public void reconnect();

    /**
     * 描述: 是否延迟加载
     *
     * @author ZhangYi
     * @date 2019-10-25 12:30:34
     */
    public boolean delay();

    @Override
    default public void destroy() throws Exception {
        close();
    }

    /**
     * @throws InterruptedException
     * @throws Exception
     * @描述 切换CPU负荷(防止CPU负荷100 %)
     * @author ZhangYi
     * @时间 2020年10月22日 上午9:59:27
     */
    default public void release() throws InterruptedException {
        Thread.sleep(3);
    }

    /**
     * 描述: 检查连接是否连通
     *
     * @param uris 多地址以','分割(地址与端口以':'分割)
     * @return
     * @author ZhangYi
     * @date 2020-06-12 17:11:02
     */
//    default public boolean ping(String uris) {
//        return HttpUtils.isConnected(uris);
//    }
}
