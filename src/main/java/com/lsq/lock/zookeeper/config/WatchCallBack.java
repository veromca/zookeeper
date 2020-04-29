package com.lsq.lock.zookeeper.config;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class WatchCallBack implements Watcher, AsyncCallback.StatCallback,AsyncCallback.DataCallback {
    ZooKeeper zk;
    MyConf conf;
    CountDownLatch cc = new CountDownLatch(1);
    @Override
    public void processResult(int i, String s, Object o, byte[] data, Stat stat) {
        if(null != data){
            String str = new String(data);
            conf.setConf(str);
            cc.countDown();
        }
    }

    public void aWait(){
        //设置监控 检测节点是否存在 (由于zookeeper的监控都是一次性的所以 每次必须设置监控)
        //exits 方法的意义在于无论节点是否存在，都可以进行注册 watcher，能够对节点的创建，删除和修改进行监听，但是其子节点发生各种变化，都不会通知客户端。
        zk.exists("/AppConf",this,this,"sdf");
        try {
            cc.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processResult(int i, String s, Object o, Stat stat) {
        if(stat != null){
            zk.getData("/AppConf",this,this,"sad");
        }
    }

    // Watcher 监听
    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                break;
            case NodeCreated: //节点创建事件
                zk.getData("/AppConf",this,this,"sad");//获取数据
                break;
            case NodeDeleted:
                // 容忍性
                conf.setConf("");
                cc = new CountDownLatch(1);
                break;
            case NodeDataChanged://节点修改事件
                zk.getData("/AppConf",this,this,"sad");//获取数据
                break;
            case NodeChildrenChanged:
                // 子节点修改
                break;
        }
    }

    public MyConf getConf() {
        return conf;
    }

    public void setConf(MyConf conf) {
        this.conf = conf;
    }

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }


}
