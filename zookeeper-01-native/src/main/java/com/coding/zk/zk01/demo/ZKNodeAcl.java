package com.coding.zk.zk01.demo;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 操作节点acl演示
 */
@Slf4j
@Getter
@Setter
public class ZKNodeAcl implements Watcher {

    private ZooKeeper zookeeper = null;

    public static final String zkServerPath = "emon:2181";
    public static final Integer timeout = 5000;

    public ZKNodeAcl() {}

    public ZKNodeAcl(String connectString) {
        try {
            ZKNodeAcl zkNodeAcl = new ZKNodeAcl();
            zookeeper = new ZooKeeper(connectString, timeout, zkNodeAcl);
            zkNodeAcl.setZookeeper(zookeeper);
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

    public void createZKNode(String path, byte[] data, List<ACL> acls) {

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
            result = zookeeper.create(path, data, acls, CreateMode.PERSISTENT);
            log.warn("创建节点：\t" + result + "\t成功...");
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        ZKNodeAcl zkServer = new ZKNodeAcl(zkServerPath);

        /*
         * ====================== 创建node start ======================
         */
        // acl 任何人都可以访问
        // zkServer.createZKNode("/acltest", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        // 自定义用户认证访问
        /*List<ACL> acls = new ArrayList<>();
        Id emon1 = new Id("digest", AclUtils.getDigestUserPwd("emon1:123456"));
        Id emon2 = new Id("digest", AclUtils.getDigestUserPwd("emon2:123456"));
        acls.add(new ACL(ZooDefs.Perms.ALL, emon1));
        acls.add(new ACL(ZooDefs.Perms.READ, emon2));
        acls.add(new ACL(ZooDefs.Perms.DELETE | ZooDefs.Perms.CREATE, emon2));
        zkServer.createZKNode("/acltest/testdigest", "testdigest".getBytes(), acls);*/

        // 注册过的用户必须通过addAuthInfo才能操作节点，参考命令行 addauth
        /*zkServer.getZookeeper().addAuthInfo("digest", "emon1:123456".getBytes());
        Stat exists = zkServer.getZookeeper().exists("/acltest/testdigest/childtest", false);
        if (exists != null) {
            zkServer.getZookeeper().delete("/acltest/testdigest/childtest", exists.getVersion());
        }
        zkServer.createZKNode("/acltest/testdigest/childtest", "childtest".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL);
        Stat stat = new Stat();
        byte[] data = zkServer.getZookeeper().getData("/acltest/testdigest", false, stat);
        log.warn(new String(data));
        zkServer.getZookeeper().setData("/acltest/testdigest", "now".getBytes(), stat.getVersion());*/

        // ip方式的acl，注意：ip是指当前机器的ip
        /*List<ACL> aclsIP = new ArrayList<>();
        Id ipId1 = new Id("ip", "192.168.200.1");
        aclsIP.add(new ACL(ZooDefs.Perms.ALL, ipId1));
        zkServer.createZKNode("/iptest", "iptest".getBytes(), aclsIP);*/

        // 验证ip是否有权限
        /*Stat stat = new Stat();
        byte[] data = zkServer.getZookeeper().getData("/iptest", false, stat);
        System.out.println(new String(data));
        System.out.println(stat.getVersion());
        
        zkServer.getZookeeper().setData("/iptest", "now".getBytes(), stat.getVersion());
        data = zkServer.getZookeeper().getData("/iptest", false, stat);
        System.out.println(new String(data));
        System.out.println(stat.getVersion());*/
    }

    @Override
    public void process(WatchedEvent event) {

    }
}