package com.lsq.lock.zookeeper.distributedlock;

import com.lsq.lock.zookeeper.config.ZKUtils;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 基于zk实现分布式锁
 *
 *
 */
public class TestLock {
    ZooKeeper zk;

    @Before
    public void conn(){
        zk= ZKUtils.getZK();
    }

    @After
    public void close(){
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void lock(){
        for (int i = 0; i < 3; i++) {
           new Thread(){
                @Override
                public void run(){
                    WatchCallBack watchCallBack = new WatchCallBack();
                    watchCallBack.setZk(zk);
                    watchCallBack.setThreadName(Thread.currentThread().getName());
                    watchCallBack.tryLock();//上锁

                    System.out.println(Thread.currentThread().getName()+" working...");//执行任务

                    watchCallBack.unLock();// 释放锁
                }
            }.start();
        }

        while (true){
            //等待线程执行完
        }

    }
}
