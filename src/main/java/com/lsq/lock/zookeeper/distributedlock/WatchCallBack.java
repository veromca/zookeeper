package com.lsq.lock.zookeeper.distributedlock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 采用zk的临时顺序节点实现分布式锁
 * Zookeeper中临时顺序节点的特性：
 * 第一，节点的生命周期和client回话绑定，即创建节点的客户端回话一旦失效，那么这个节点就会被删除。（临时性）
 * 第二，每个父节点都会维护子节点创建的先后顺序，自动为子节点分配一个×××数值，以后缀的形式自动追加到节点名称中，作为这个节点最终的节点名称。（顺序性）
 * 基于临时顺序节点的特性，Zookeeper实现分布式锁的思路如下：
 * 1.client调用create()方法创建“/root/lock_”节点，注意节点类型是EPHEMERAL_SEQUENTIAL
 * 2.client调用getChildren("/root/lock_",false)来获取所有已经创建的子节点，这里并不注册任何Watcher
 * 3.客户端获取到所有子节点Path后，如果发现自己在步骤1中创建的节点是所有节点中最小的，那么就认为这个客户端获得了锁
 * 4.如果在步骤3中，发现不是最小的，那么找到比自己小的那个节点，然后对其调用exist()方法注册事件监听
 * 5.之后一旦这个被关注的节点移除，客户端会收到相应的通知，这个时候客户端需要再次调用getChildren("/root/lock_",false)来确保自己是最小的节点，然后进入步骤3
 */
public class WatchCallBack implements Watcher, AsyncCallback.StringCallback, AsyncCallback.Children2Callback, AsyncCallback.StatCallback {

    ZooKeeper zk;
    String threadName;
    CountDownLatch cc = new CountDownLatch(1);
    String pathName;

    public String getPathName() {
        return pathName;
    }

    public void setPathName(String pathName) {
        this.pathName = pathName;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    /**
     * 加锁
     * 1. client调用create()方法创建“/root/lock”节点，注意节点类型是EPHEMERAL_SEQUENTIAL
     */
    public void tryLock() {
        System.out.println(threadName + " create....");
        //OPEN_ACL_UNSAFE使所有ACL都“开放”了：任何应用程序在节点上可进行任何操作，能创建、列出和删除它的子节点
        //ZOO_READ_ACL_UNSAFE 对任何应用程序，是只读的
        //CREATE_ALL_ACL赋予了节点的创建者所有的权限，在创建者采用此ACL创建节点之前，已经被服务器所认证（例如，采用 “ digest”方案）
        zk.create("/lock", threadName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, this, "abc");
        try {
            cc.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 解锁
     * 删除节点
     */
    public void unLock() {
        try {
            zk.delete(pathName, -1);
            System.out.println(threadName + " over work....");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建节点事件回调
     * 2.client调用getChildren("/",false)来获取所有已经创建的子节点，这里并不注册任何Watcher
     * @param i
     * @param s
     * @param o
     * @param name
     */
    @Override
    public void processResult(int i, String s, Object o, String name) {
        if (name != null) {
            System.out.println(threadName + "  create node : " + name);
            pathName = name;
            //监控的就是该path节点下的子节点的变化（子节点的创建、修改、删除都会监控到)
            zk.getChildren("/", false, this, "sdf");
        }
    }

    /**
     * 5.之后一旦这个被关注的节点移除，客户端会收到相应的通知，这个时候客户端需要再次调用getChildren("/root/lock_",false)来确保自己是最小的节点，然后进入步骤3
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        //如果第一个哥们，那个锁释放了，其实只有第二个收到了回调事件！！
        //如果，不是第一个哥们，某一个，挂了，也能造成他后边的收到这个通知，从而让他后边那个跟去watch挂掉这个哥们前边的。。。
        switch (event.getType()) {
            case None:
                break;
            case NodeCreated:
                break;
            case NodeDeleted:
                zk.getChildren("/", false, this, "sdf");
                break;
            case NodeDataChanged:
                break;
            case NodeChildrenChanged:
                break;
        }
    }

    /**
     * 获取子节点回调
     * 3.客户端获取到所有子节点Path后，如果发现自己在步骤1中创建的节点是所有节点中最小的，那么就认为这个客户端获得了锁
     * 4.如果在步骤3中，发现不是最小的，那么找到比自己小的那个节点，然后对其调用exist()方法注册事件监听
     * @param rc
     * @param s
     * @param o
     * @param children
     * @param stat
     */
    @Override
    public void processResult(int rc, String s, Object o, List<String> children, Stat stat) {
        //一定能看到自己前边的。。

        //System.out.println(threadName+" look locks.....");
        /*for (String child : children) {
            System.out.println(child);
        }*/

        Collections.sort(children);
        int i = children.indexOf(pathName.substring(1));

        // 判断是不是第一个
        if (i == 0) {
            System.out.println(threadName + " i am first...");
            try {
                zk.setData("/", threadName.getBytes(), -1);
                cc.countDown();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } else {
            // 不是，监控上一个节点（比自己小）
            zk.exists("/" + children.get(i - 1), this, this, "sdf");
        }

    }

    @Override
    public void processResult(int i, String s, Object o, Stat stat) {
        // exists 回调
    }
}
