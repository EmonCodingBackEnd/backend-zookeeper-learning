package com.coding.zk.spring.zk01.demo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.coding.zk.spring.zk01.demo.callback.CreateCallBack;
import com.coding.zk.spring.zk01.demo.callback.DeleteCallBack;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 操作演示
 */
@Slf4j
@Getter
@Setter
public class ZKNodeOperator implements Watcher {
    private ZooKeeper zookeeper = null;

    public static final String zkServerPath = "emon:2181";
    public static final Integer timeout = 5000;

    private CountDownLatch cdl;

    public ZKNodeOperator(CountDownLatch cdl) {
        this.cdl = cdl;
    }

    public ZKNodeOperator(String connectString) {
        cdl = new CountDownLatch(1);
        try {
            // [lm's ps]: 20230208 08:52 注意，原生API实例化是异步的
            zookeeper = new ZooKeeper(connectString, timeout, new ZKNodeOperator(cdl));
        } catch (IOException e) {
            log.error("异常", e);
            if (zookeeper != null) {
                try {
                    zookeeper.close();
                } catch (InterruptedException e1) {
                    log.error("异常1", e1);
                }
            }
        }

        try {
            cdl.await();
            log.warn("创建连接成功");
        } catch (InterruptedException e) {
            log.error("创建连接异常", e);
        }
    }

    /**
     *
     * 创建zk节点
     */
    public void createZKNode(String path, byte[] data, List<ACL> acls, boolean async) {

        String result;
        try {
            /*
             * 同步或者异步创建节点，都不支持子节点的递归创建，异步有一个callback函数
             * 参数：
             * path：创建的路径
             * data：存储的数据的byte[]
             * acl：控制权限策略
             * 			Ids.OPEN_ACL_UNSAFE --> world:anyone:cdrwa
             * 			CREATOR_ALL_ACL --> auth:user:password:cdrwa
             * createMode：节点类型, 是一个枚举
             * 			PERSISTENT：持久节点
             * 			PERSISTENT_SEQUENTIAL：持久顺序节点
             * 			EPHEMERAL：临时节点
             * 			EPHEMERAL_SEQUENTIAL：临时顺序节点
             */
            if (!async) {
                result = zookeeper.create(path, data, acls, CreateMode.PERSISTENT);
                log.warn("创建节点：\t" + result + "\t成功...");
            } else {
                String ctx = "{'create':'success'}";
                zookeeper.create(path, data, acls, CreateMode.PERSISTENT, new CreateCallBack(), ctx);
            }

            Thread.sleep(2000);
        } catch (Exception e) {
            log.error("创建节点异常", e);
        }
    }

    public static void main(String[] args) throws Exception {
        boolean async = true;

        ZKNodeOperator zkServer = new ZKNodeOperator(zkServerPath);
        // 创建zk节点
        // zkServer.createZKNode("/testnode", "testnode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, async);

        /*
         * 参数： path：节点路径 data：数据 version：数据状态
         */
        // Stat status = zkServer.getZookeeper().setData("/testnode", "xyz".getBytes(), 0);
        // log.warn(status.getVersion());

        /*
         * 参数： path：节点路径 version：数据状态
         */
        zkServer.createZKNode("/test-delete-node", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, async);
        if (!async) {
            zkServer.getZookeeper().delete("/test-delete-node", 0);
        } else {
            String ctx = "{'delete':'success'}";
            zkServer.getZookeeper().delete("/test-delete-node", 0, new DeleteCallBack(), ctx);
        }

        Thread.sleep(2000);
    }

    @Override
    public void process(WatchedEvent event) {
        if (Event.KeeperState.SyncConnected.equals(event.getState())) {
            cdl.countDown();
        }
    }
}
