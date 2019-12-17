package com.yf.bigdata.hadoop.config;

import com.yf.bigdata.hadoop.utils.HDFSIO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * @Author: YangFei
 * @Description: HDFS相关配置
 * @create: 2019-12-13 10:26
 */
@Configuration
public class HDFSConfig {

    @Value("${hdfs.defaultFS}")
    private String HDFS_PATH;
    @Value("${hdfs.user}")
    private String HDFS_USER;
    @Value("${dfs.nameservices}")
    private String dfs_nameservices;
    @Value("${dfs.ha.namenodes.cluster}")
    private String dfs_ha_namenodes_cluster;
    @Value("${dfs.namenode.rpc-address.cluster.nn1}")
    private String dfs_namenode_rpc_address_cluster_nn1;
    @Value("${dfs.namenode.rpc-address.cluster.nn2}")
    private String dfs_namenode_rpc_address_cluster_nn2;
    @Value("${dfs.client.failover.proxy.provider.cluster}")
    private String dfs_client_failover_proxy_provider_cluster;


    @Bean
    public HDFSIO getService() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", HDFS_PATH);
        conf.set("dfs.nameservices", dfs_nameservices);
        conf.set("dfs.ha.namenodes.cluster", dfs_ha_namenodes_cluster);
        conf.set("dfs.namenode.rpc-address.cluster.nn1", dfs_namenode_rpc_address_cluster_nn1);
        conf.set("dfs.namenode.rpc-address.cluster.nn2", dfs_namenode_rpc_address_cluster_nn2);
        conf.set("dfs.client.failover.proxy.provider.cluster", dfs_client_failover_proxy_provider_cluster);
        return new HDFSIO(conf, HDFS_PATH, HDFS_USER);
    }
}
