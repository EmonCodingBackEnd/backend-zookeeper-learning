package com.coding.zk.zk01.demo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

import com.coding.zk.zk01.demo.callback.Children2CallBack;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 获取子节点数据的演示
 */
@Slf4j
@Getter
@Setter
public class ZKGetChildrenList implements Watcher {

    private ZooKeeper zookeeper = null;

    public static final String zkServerPath = "emon:2181";
    public static final Integer timeout = 5000;

    public ZKGetChildrenList() {}

    public ZKGetChildrenList(String connectString) {
        try {
            ZKGetChildrenList zkGetChildrenList = new ZKGetChildrenList();
            zookeeper = new ZooKeeper(connectString, timeout, zkGetChildrenList);
            zkGetChildrenList.setZookeeper(zookeeper);
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

        ZKGetChildrenList zkServer = new ZKGetChildrenList(zkServerPath);

        /*
         * 参数： path：父节点路径 watch：true或者false，注册一个watch事件
         */
        /*List<String> strChildList = zkServer.getZookeeper().getChildren("/test", true);
        for (String childernStr : strChildList) {
            log.warn(childernStr);
        }*/

        // 异步调用
        String ctx = "{'callback':'ChildrenCallback'}";
        // zkServer.getZookeeper().getChildren("/test", true, new ChildrenCallBack(), ctx);
        zkServer.getZookeeper().getChildren("/test2", true, new Children2CallBack(), ctx);

        countDown.await();
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            if (event.getType() == EventType.NodeChildrenChanged) {
                log.warn("NodeChildrenChanged");
                List<String> strChildList = zookeeper.getChildren(event.getPath(), false);
                for (String childernStr : strChildList) {
                    log.warn(childernStr);
                }
                countDown.countDown();
            } else if (event.getType() == EventType.NodeCreated) {
                log.warn("NodeCreated");
            } else if (event.getType() == EventType.NodeDataChanged) {
                log.warn("NodeDataChanged");
            } else if (event.getType() == EventType.NodeDeleted) {
                log.warn("NodeDeleted");
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
