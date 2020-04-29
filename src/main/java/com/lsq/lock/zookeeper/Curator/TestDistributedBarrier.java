package com.lsq.lock.zookeeper.Curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * 使用 curator 的分布式锁，对于监听的相同节点，若之前发生了变更，之后连接还会貌似数据恢复一点，数据同步，相当于重复注册，当前 /super 节点，持续订阅服务，
 *
 * curator 人性化操作：
 *
 * 对于一个节点的CRUD监控
 * 实现分布式锁
 * 实现barrier、原子计数器
 * 实现队列
 */
public class TestDistributedBarrier {

    /**
     * zookeeper地址
     */
    static final String CONNECT_ADDR = "192.168.101.100:2181,192.168.101.101:2181,192.168.101.102:2181";
    /**
     * session超时时间
     */
    static final int SESSION_OUTTIME = 5000;//ms

    static DistributedBarrier barrier = null;

    public static void main(String[] args) throws Exception {

        for (int i = 0; i < 5; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        //重试策略: baseSleepTimeMs 初始睡眠时间,用于计算之后的每次重试的sleep时间;maxRetries 最大重试次数
                        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
                        CuratorFramework cf = CuratorFrameworkFactory.builder()
                                .connectString(CONNECT_ADDR)
                                .sessionTimeoutMs(SESSION_OUTTIME)
                                .retryPolicy(retryPolicy)
                                .build();
                        cf.start();
                        barrier = new DistributedBarrier(cf, "/locktest");
                        System.out.println(Thread.currentThread().getName() + "设置barrier!");
                        barrier.setBarrier();    //设置
                       // barrier.waitOnBarrier();    //等待
                        System.out.println(Thread.currentThread().getName() + "---------开始执行程序----------");
                        System.out.println(Thread.currentThread().getName() + "执行完成!");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, "t" + i).start();
        }

        Thread.sleep(5000);
        barrier.removeBarrier();    //释放


    }
}
