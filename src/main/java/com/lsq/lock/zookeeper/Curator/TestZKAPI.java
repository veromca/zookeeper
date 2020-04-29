package com.lsq.lock.zookeeper.Curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Apache Curator是一个比较完善的ZooKeeper客户端框架
 * 封装ZooKeeper client与ZooKeeper server之间的连接处理
 * 提供了一套Fluent风格的操作API
 * 提供ZooKeeper各种应用场景(recipe， 比如：分布式锁服务、集群领导选举、共享计数器、缓存机制、分布式队列等)的抽象封装
 *
 * Curator主要从以下几个方面降低了zk使用的复杂性：
 * 重试机制:提供可插拔的重试机制, 它将给捕获所有可恢复的异常配置一个重试策略，并且内部也提供了几种标准的重试策略(比如指数补偿)
 * 连接状态监控: Curator初始化之后会一直对zk连接进行监听，一旦发现连接状态发生变化将会作出相应的处理
 * zk客户端实例管理:Curator会对zk客户端到server集群的连接进行管理，并在需要的时候重建zk实例，保证与zk集群连接的可靠性
 * 各种使用场景支持:Curator实现了zk支持的大部分使用场景（甚至包括zk自身不支持的场景），这些实现都遵循了zk的最佳实践，并考虑了各种极端情况
 */
public class TestZKAPI {
    /**
     * zookeeper地址
     */
    static final String CONNECT_ADDR = "192.168.101.100:2181,192.168.101.101:2181,192.168.101.102:2181";

    public static void main(String[] args) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(5000)// 会话超时时间
                .connectionTimeoutMs(5000)// 连接超时时间
                .retryPolicy(retryPolicy)
                .namespace("base")// 包含隔离名称
                .build();
        client.start();

        try {
            // 创建数据节点
            /*client.create().creatingParentsIfNeeded() // 递归创建所需父节点
                    .withMode(CreateMode.PERSISTENT) // 创建类型为持久节点
                    .forPath("/nodeA", "init".getBytes()); // 目录及内容*/

            //获取节点数据
            byte[] bytes = client.getData().forPath("/nodeA");
            System.out.println(new String(bytes));
            /*
            //修改节点数据
            client.setData()
                    .withVersion(0) // 指定版本修改
                    .forPath("/nodeA", "data".getBytes());

            //删除数据节点
            client.delete()
                    .guaranteed()  // 强制保证删除
                    .deletingChildrenIfNeeded() // 递归删除子节点
                    .withVersion(1) // 指定删除的版本号
                    .forPath("/nodeA");

            //事务
            client.inTransaction().check().forPath("/nodeA")
                    .and()
                    .create().withMode(CreateMode.EPHEMERAL).forPath("/nodeB", "init".getBytes())
                    .and()
                    .create().withMode(CreateMode.EPHEMERAL).forPath("/nodeC", "init".getBytes())
                    .and()
                    .commit();*/

            Stat stat1 = client.checkExists() // 检查是否存在
                    .forPath("/nodeA");
            System.out.println("'/nodeA'是否存在： " + (stat1 != null ? true : false));
            System.out.println(client.getChildren().forPath("/nodeA")); // 获取子节点的路径

            //异步回调， 异步创建节点
            Executor executor = Executors.newFixedThreadPool(2);
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .inBackground((curatorFramework, curatorEvent) -> {
                        System.out.println(String.format("eventType:%s,resultCode:%s",curatorEvent.getType(),curatorEvent.getResultCode()));
                    },executor)
                    .forPath("/nodeC" ,"nodeC".getBytes());
            Thread.sleep(100);



            //Curator提供了三种Watcher(Cache)来监听结点的变化：

            //Path Cache：监视一个路径下1）孩子结点的创建、2）删除，3）以及结点数据的更新。产生的事件会传递给注册的PathChildrenCacheListener。
            //Node Cache：监视一个结点的创建、更新、删除，并将结点的数据缓存在本地。
            //Tree Cache：Path Cache和Node Cache的“合体”，监视路径下的创建、更新、删除事件，并缓存路径下所有孩子结点的数据。
            /**
             * 在注册监听器的时候，如果传入此参数，当事件触发时，逻辑由线程池处理
             */
            ExecutorService pool = Executors.newFixedThreadPool(2);
            /**
             * 监听数据节点的变化情况
             */
            final NodeCache nodeCache = new NodeCache(client, "nodeA", false);
            nodeCache.start(true);
            nodeCache.getListenable().addListener(
                    new NodeCacheListener() {
                        @Override
                        public void nodeChanged() throws Exception {
                            System.out.println("nodeA data is changed, new data: " +
                                    new String(nodeCache.getCurrentData().getData()));
                        }
                    },
                    pool
            );

            /**
             * 监听子节点的变化情况
             */
            final PathChildrenCache childrenCache;
            childrenCache = new PathChildrenCache(client, "/nodeA", true);
            childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            childrenCache.getListenable().addListener(
                    new PathChildrenCacheListener() {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                                throws Exception {
                            switch (event.getType()) {
                                case CHILD_ADDED:
                                    System.out.println("CHILD_ADDED: " + event.getData().getPath());
                                    break;
                                case CHILD_REMOVED:
                                    System.out.println("CHILD_REMOVED: " + event.getData().getPath());
                                    break;
                                case CHILD_UPDATED:
                                    System.out.println("CHILD_UPDATED: " + event.getData().getPath());
                                    break;
                                default:
                                    break;
                            }
                        }
                    },
                    pool
            );

            Thread.sleep(10 * 1000);
            pool.shutdown();

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
