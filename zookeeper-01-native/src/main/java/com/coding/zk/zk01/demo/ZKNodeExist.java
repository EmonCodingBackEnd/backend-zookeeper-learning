package com.coding.zk.zk01.demo;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 判断节点是否存在演示
 */
@Slf4j
@Getter
@Setter
public class ZKNodeExist implements Watcher {

    private ZooKeeper zookeeper = null;

    public static final String zkServerPath = "emon:2181";
    public static final Integer timeout = 5000;

    public ZKNodeExist() {}

    public ZKNodeExist(String connectString) {
        try {
            ZKNodeExist zkNodeExist = new ZKNodeExist();
            zookeeper = new ZooKeeper(connectString, timeout, zkNodeExist);
            zkNodeExist.setZookeeper(zookeeper);
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
    }

    private static CountDownLatch countDown = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {

        ZKNodeExist zkServer = new ZKNodeExist(zkServerPath);

        /*
         * 参数： path：节点路径 watch：watch
         */
        Stat stat = zkServer.getZookeeper().exists("/test", true);
        if (stat != null) {
            log.warn("查询的节点版本为dataVersion：" + stat.getVersion());
        } else {
            log.warn("该节点不存在...");
        }

        countDown.await();
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == EventType.NodeCreated) {
            log.warn("节点创建");
            countDown.countDown();
        } else if (event.getType() == EventType.NodeDataChanged) {
            log.warn("节点数据改变");
            countDown.countDown();
        } else if (event.getType() == EventType.NodeDeleted) {
            log.warn("节点删除");
            countDown.countDown();
        }
    }

}
