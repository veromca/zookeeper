package com.lsq.lock.zookeeper.reentrantlock;

import com.lsq.lock.zookeeper.configcenter.DefaultWatch;
import com.lsq.lock.zookeeper.configcenter.ZKConf;
import com.lsq.lock.zookeeper.configcenter.ZKUtils;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestLock {


    ZooKeeper zk;
    ZKConf zkConf;
    DefaultWatch defaultWatch;

    @Before
    public void conn(){
        zkConf = new ZKConf();
        zkConf.setAddress("192.168.101.100:2181,192.168.101.101:2181,192.168.101.102:2181/testLock");
        zkConf.setSessionTime(1000);
        defaultWatch = new DefaultWatch();
        ZKUtils.setConf(zkConf);
        ZKUtils.setWatch(defaultWatch);
        zk = ZKUtils.getZK();
    }

    @After
    public void close(){
        ZKUtils.closeZK();
    }

    @Test
    public void testlock(){
        for (int i = 0; i < 3; i++) {
            new Thread(){
                @Override
                public void run() {
                    WatchCallBack watchCallBack = new WatchCallBack();
                    watchCallBack.setZk(zk);
                    String name = Thread.currentThread().getName();
                    watchCallBack.setThreadName(name);

                    try {
                        //tryLock
                        watchCallBack.tryLock();
                        System.out.println(name + " at work");
                        watchCallBack.getRootData();
//                        Thread.sleep(1000);
                        //unLock
                        watchCallBack.unLock();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }.start();
        }
       while(true){

       }
    }
}
