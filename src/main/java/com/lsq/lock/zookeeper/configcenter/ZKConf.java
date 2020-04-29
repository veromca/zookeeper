package com.lsq.lock.zookeeper.configcenter;


public class ZKConf {

    private String address;
    private Integer sessionTime;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Integer getSessionTime() {
        return sessionTime;
    }

    public void setSessionTime(Integer sessionTime) {
        this.sessionTime = sessionTime;
    }
}
