package com.coding.zk.spring.zk01.demo;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 获取节点数据的演示
 */
@Getter
@Setter
@Slf4j
public class ZKGetNodeData implements Watcher {

    private ZooKeeper zookeeper = null;

    public static final String zkServerPath = "emon:2181";
    public static final Integer timeout = 5000;
    private static Stat stat = new Stat();

    public ZKGetNodeData() {}

    public ZKGetNodeData(String connectString) {
        try {
            ZKGetNodeData zkGetNodeData = new ZKGetNodeData();
            zookeeper = new ZooKeeper(connectString, timeout, zkGetNodeData);
            zkGetNodeData.setZookeeper(zookeeper);
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

        ZKGetNodeData zkServer = new ZKGetNodeData(zkServerPath);

        /*
         * 参数： path：节点路径 watch：true或者false，注册一个watch事件 stat：状态
         */
        byte[] resByte = zkServer.getZookeeper().getData("/test", true, stat);
        if (resByte != null) {
            String result = new String(resByte);
            System.out.println("当前值:" + result);
        }
        countDown.await();
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            if (event.getType() == EventType.NodeDataChanged) {
                byte[] resByte = zookeeper.getData("/test", false, stat);
                String result = new String(resByte);
                System.out.println("更改后的值:" + result);
                System.out.println("版本号变化dversion：" + stat.getVersion());
                countDown.countDown();
            } else if (event.getType() == EventType.NodeCreated) {

            } else if (event.getType() == EventType.NodeChildrenChanged) {

            } else if (event.getType() == EventType.NodeDeleted) {

            }
        } catch (KeeperException | InterruptedException e) {
            log.error("", e);
        }
    }

}
