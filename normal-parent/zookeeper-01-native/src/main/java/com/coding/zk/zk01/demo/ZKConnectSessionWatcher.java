package com.coding.zk.zk01.demo;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import lombok.extern.slf4j.Slf4j;

/**
 * 恢复之前的会话连接演示
 */
@Slf4j
public class ZKConnectSessionWatcher implements Watcher {

    public static final String zkServerPath = "emon:2181";
    // public static final String zkServerPath = "emon:2181,emon2:2181,emon3:2181";
    public static final Integer timeout = 5000;

    public static void main(String[] args) throws Exception {
        log.warn("客户端开始连接zookeeper服务器...");
        ZooKeeper zk = new ZooKeeper(zkServerPath, timeout, new ZKConnectSessionWatcher());

        long sessionId = zk.getSessionId();
        byte[] sessionPassword = zk.getSessionPasswd();
        String sexSessionId = "0x" + Long.toHexString(sessionId);
        log.warn("sexSessionId={}", sexSessionId);

        log.warn("连接状态：{}", zk.getState());
        Thread.sleep(1000);
        log.warn("连接状态：{}", zk.getState());

        // 开始会话重连
        Thread.sleep(200);
        log.warn("开始会话重连...");
        ZooKeeper zkSession =
            new ZooKeeper(zkServerPath, timeout, new ZKConnectSessionWatcher(), sessionId, sessionPassword);

        log.warn("重新连接状态zkSession：{}", zkSession.getState());
        Thread.sleep(1000);
        log.warn("重新连接状态zkSession：{}", zkSession.getState());

        zk.close();
    }

    @Override
    public void process(WatchedEvent event) {
        log.warn("接受到watch通知：{}", event);
    }
}
