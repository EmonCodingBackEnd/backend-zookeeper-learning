package com.coding.zk.zk02.curator;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

import com.coding.zk.zk02.utils.AclUtils;

public class CuratorAcl {

    public CuratorFramework client;
    public static final String zkServerPath = "emon:2181";

    public CuratorAcl() {
        RetryPolicy retryPolicy = new RetryNTimes(3, 5000);
        client = CuratorFrameworkFactory.builder()
            // 登录
            .authorization("digest", "emon1:123456".getBytes())
            // 英文逗号分隔的服务器的连接地址
            .connectString(zkServerPath)
            // 会话超时时间，默认60秒
            .sessionTimeoutMs(60000)
            // 重试策略
            .retryPolicy(retryPolicy)
            // client的所有操作都局限于该命名空间之下
            .namespace("workspace")
            //
            .ensembleTracker(false).build();
        client.start();
    }

    public void closeZKClient() {
        if (client != null) {
            this.client.close();
        }
    }

    public static void main(String[] args) throws Exception {
        // 实例化
        CuratorAcl cto = new CuratorAcl();
        boolean isZkCuratorStarted = cto.client.isStarted();
        CuratorFrameworkState state = cto.client.getState();
        isZkCuratorStarted = CuratorFrameworkState.STARTED.equals(state);
        System.out.println("当前客户的状态：" + (isZkCuratorStarted ? "连接中" : "已关闭") + "==>当前客户的状态码：" + state.name());

        String nodePath = "/acl/father/child/sub";

        List<ACL> acls = new ArrayList<>();
        Id emon1 = new Id("digest", AclUtils.getDigestUserPwd("emon1:123456"));
        Id emon2 = new Id("digest", AclUtils.getDigestUserPwd("emon2:123456"));
        acls.add(new ACL(Perms.ALL, emon1));
        acls.add(new ACL(Perms.READ, emon2));
        acls.add(new ACL(Perms.DELETE | Perms.CREATE, emon2));

        // 创建节点
        Stat statExist = cto.client.checkExists().forPath("/acl");
        if (statExist != null) {
            cto.client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/acl");
        }

        byte[] data = "spiderman".getBytes();
        cto.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
            // applyToParents=true表示会应用acl到新创建的父节点，不影响之前的父节点
            .withACL(acls/*, true*/).forPath(nodePath, data);

        // 设置权限
        // cto.client.setACL().withACL(acls).forPath("/acl");

        // 更新节点数据
        /*byte[] newData = "batman".getBytes();
        cto.client.setData().forPath(nodePath, newData);*/

        // 读取节点数据
        /*Stat stat = new Stat();
        byte[] data = cto.client.getData().storingStatIn(stat).forPath(nodePath);
        System.out.println("节点" + nodePath + "的数据为: " + new String(data));
        System.out.println("该节点的版本号为: " + stat.getVersion());*/

        // 删除节点
        // cto.client.delete().guaranteed().deletingChildrenIfNeeded().forPath(nodePath);

        cto.closeZKClient();
        boolean isZkCuratorStarted2 = cto.client.isStarted();
        CuratorFrameworkState state2 = cto.client.getState();
        isZkCuratorStarted2 = CuratorFrameworkState.STARTED.equals(state);
        System.out.println("当前客户的状态：" + (isZkCuratorStarted2 ? "连接中" : "已关闭") + "==>当前客户的状态码：" + state2.name());
    }

}
