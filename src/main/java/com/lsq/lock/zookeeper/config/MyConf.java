package com.lsq.lock.zookeeper.config;

import lombok.Generated;

/**
 * 自定义配置
 * 可以是IP,服务名称等
 * 
 */
public class MyConf {
    private String conf;

    public String getConf() {
        return conf;
    }

    public void setConf(String conf) {
        this.conf = conf;
    }
}
