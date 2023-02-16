package com.coding.zk.zk02.curator;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

public class CuratorOperator {

    public CuratorFramework client;
    public static final String zkServerPath = "emon:2181";

    private final static Map<String, Closeable> caches = new HashMap<>();

    /**
     * 实例化zk客户端
     */
    public CuratorOperator() throws InterruptedException {
        /*【推荐】
         * 同步创建zk示例，原生api是异步的
         *
         * curator连接zookeeper的策略:ExponentialBackoffRetry
         * baseSleepTimeMs：初始sleep的时间
         * maxRetries：最大重试次数
         * maxSleepMs：最大sleep时间，默认Integer.MAX_VALUE
         * 比如：初始重试间隔为1000ms，首次失败后，触发重试；第一次重试失败后，等待1000ms*2之后重试；第二次重试失败后，等待1000ms*2*2之后重试；
         * 也即指数级延长了重试时间间隔。如果重试时间间隔大于了maxSleepMs，则按照maxSleepMs取值。
         * 时间间隔 = baseSleepTimeMs * Math.max(1, random.nextInt(1 << (retryCount + 1)))
         */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);

        /*【推荐
         * curator连接zookeeper的策略:RetryNTimes
         * n：重试的次数
         * sleepMsBetweenRetries：每次重试间隔的时间
         */
        // RetryPolicy retryPolicy = new RetryNTimes(3, 5000);

        /*
         * curator连接zookeeper的策略:RetryOneTime
         * sleepMsBetweenRetry:每次重试间隔的时间
         */
        // RetryPolicy retryPolicy2 = new RetryOneTime(3000);

        /*
         * 永远重试，不推荐使用
         */
        // RetryPolicy retryPolicy3 = new RetryForever(retryIntervalMs)

        /*
         * curator连接zookeeper的策略:RetryUntilElapsed
         * maxElapsedTimeMs:最大重试时间
         * sleepMsBetweenRetries:每次重试间隔
         * 重试时间超过maxElapsedTimeMs后，就不再重试
         * 以sleepMsBetweenRetries的间隔重连,直到超过maxElapsedTimeMs的时间设置
         */
        // RetryPolicy retryPolicy4 = new RetryUntilElapsed(2000, 3000);

        CuratorFramework client = CuratorFrameworkFactory.builder()
            // 英文逗号分隔的服务器的连接地址
            .connectString(zkServerPath)
            // 会话超时时间，默认60秒
            .sessionTimeoutMs(60000)
            // 连接创建超时时间，默认15秒
            .connectionTimeoutMs(30000)
            // 重试策略
            .retryPolicy(retryPolicy)
            // client的所有操作都局限于该命名空间之下
            .namespace("workspace")
            //
            .ensembleTracker(false).build();
        client.start();
        System.out.println("开始获取连接：" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyMMdd HH:mm:ss")));
        boolean success = client.blockUntilConnected(15000 * 5, TimeUnit.MILLISECONDS);
        if (!success) {
            System.out.println("未能等到成功！" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyMMdd HH:mm:ss")));
        } else {
            System.out.println("获取连接成功！" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyMMdd HH:mm:ss")));
        }
        this.client = client;
    }

    /**
     * 关闭zk客户端连接
     */
    public void closeZKClient() throws IOException {
        if (client == null) {
            return;
        }
        for (Map.Entry<String, Closeable> each : caches.entrySet()) {
            each.getValue().close();
        }
        waitForCacheClose();
        CloseableUtils.closeQuietly(client);
        // 重置client状态
        client = null;
    }

    /**
     * TODO 等待500ms, cache先关闭再关闭client, 否则会抛异常 因为异步处理, 可能会导致client先关闭而cache还未关闭结束. 等待Curator新版本解决这个bug.
     * BUG地址：https://issues.apache.org/jira/browse/CURATOR-157
     */
    private void waitForCacheClose() {
        try {
            Thread.sleep(500L);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws Exception {
        // 实例化
        CuratorOperator cto = new CuratorOperator();
        boolean isZkCuratorStarted = cto.client.isStarted();
        CuratorFrameworkState state = cto.client.getState();
        isZkCuratorStarted = CuratorFrameworkState.STARTED.equals(state);
        System.out.println("当前客户的状态：" + (isZkCuratorStarted ? "连接中" : "已关闭") + "==>当前客户的状态码：" + state.name());

        // 创建节点
        String nodePath = "/super/emon";
        /*byte[] data = "superme".getBytes();
        cto.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
            .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(nodePath, data);*/

        // 更新节点数据
        /*byte[] newData = "batman".getBytes();
        cto.client.setData().withVersion(0).forPath(nodePath, newData);*/

        // 读取节点数据
        /*Stat stat = new Stat();
        byte[] data = cto.client.getData()
            // 把节点信息存储到stat里面
            .storingStatIn(stat).forPath(nodePath);
        System.out.println("节点 " + nodePath + " 的数据为: " + new String(data));
        System.out.println("该节点的版本号为: " + stat.getVersion());*/

        // 删除节点
        /*cto.client.delete()
            // 如果删除失败，那么在后端还是继续会删除，直到成功
            .guaranteed()
            // 如果有子节点，就删除
            .deletingChildrenIfNeeded().withVersion(0).forPath(nodePath);*/

        // 查询子节点
        /*List<String> childNodes = cto.client.getChildren().forPath(nodePath);
        System.out.println("开始打印子节点：");
        for (String s : childNodes) {
            System.out.println(s);
        }*/

        // 判断节点是否存在,如果不存在则为空
        /*Stat statExist = cto.client.checkExists().forPath(nodePath + "/abc");
        System.out.println("是否存在？" + (statExist != null ? "是：详情" + statExist : "否"));*/

        // watcher 事件 当使用usingWatcher的时候，监听只会触发一次，监听完毕后就销毁
        /*cto.client.getData().usingWatcher(new MyCuratorWatcher()).forPath(nodePath);
        cto.client.getData().usingWatcher(new MyWatcher()).forPath(nodePath);*/

        // ==================================================华丽的分割线==================================================
        /*
         * 【为节点添加watcher】
         * NodeCache只能监听当前节点的增删改操作，不能监听子节点的事件。
         * 注意，删除时 nodeCache.getCurrentData() 为 null
         */
        /*String nodeCachePath = nodePath + "/pathChildrenCache";
        final NodeCache nodeCache = new NodeCache(cto.client, nodeCachePath);
        // buildInitial : 初始化的时候获取node的值并且缓存
        nodeCache.start(true);
        caches.put(nodeCachePath, nodeCache);
        
        if (nodeCache.getCurrentData() != null) {
            System.out.println("节点初始化数据为：" + new String(nodeCache.getCurrentData().getData()));
        } else {
            System.out.println("节点初始化数据为空...");
        }
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                if (nodeCache.getCurrentData() == null) {
                    System.out.println("空");
                    return;
                }
                String data = new String(nodeCache.getCurrentData().getData());
                System.out.println("节点路径：" + nodeCache.getCurrentData().getPath() + "数据：" + data);
            }
        });
        */

        // ==================================================华丽的分割线==================================================
        /*
        * 【为子节点添加watcher】
         * PathChildrenCache可以监听直接子节点的增、删、改事件，不能监听当前节点和孙子节点及下节点的事件。
         * 1.无法对监听路径所在节点进行监听(即不能监听path对应节点的变化)
         * 2.只能监听path对应节点下一级目录的子节点的变化内容(即只能监听/path/node1的变化，而不能监听/path/node1/node2 的变化)
         * 3.如果节点正在被监听，在命令行无法删除的，只有监听关闭，才能生效。【重要】
         */
        /*String pathChildrenCachePath = nodePath + "/pathChildrenCache";
        // cacheData=true: node contents are cached in addition to the stat（节点内容会被缓存到stat中）
        final PathChildrenCache pathChildrenCache = new PathChildrenCache(cto.client, pathChildrenCachePath, true);
        // StartMode: 初始化方式
        // POST_INITIALIZED_EVENT：异步初始化，初始化之后会触发事件
        // NORMAL：异步初始化【默认】
        // BUILD_INITIAL_CACHE：同步初始化，初始化的时候获取node的值并缓存
        PathChildrenCache.StartMode startMode = PathChildrenCache.StartMode.POST_INITIALIZED_EVENT;
        pathChildrenCache.start(startMode);
        caches.put(pathChildrenCachePath, pathChildrenCache);
        
        if (PathChildrenCache.StartMode.BUILD_INITIAL_CACHE.equals(startMode)) {
            List<ChildData> childDataList = pathChildrenCache.getCurrentData();
            System.out.println("当前数据节点的子节点数据列表：" + childDataList.size());
            for (ChildData cd : childDataList) {
                String childData = new String(cd.getData());
                System.out.println(childData);
            }
        }
        
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                // StartMode是PathChildrenCache.StartMode.POST_INITIALIZED_EVENT时才有这种事件
                if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) {
                    System.out.println("子节点初始化ok...");
                }
        
                else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                    String path = event.getData().getPath();
                    if (path.equals(pathChildrenCachePath + "/a")) {
                        System.out.println("添加子节点:" + event.getData().getPath());
                        System.out.println("子节点数据:" + new String(event.getData().getData()));
                    } else if (path.equals(pathChildrenCachePath + "/e")) {
                        System.out.println("添加不正确...");
                    }
        
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                    System.out.println("删除子节点:" + event.getData().getPath());
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                    System.out.println("修改子节点路径:" + event.getData().getPath());
                    System.out.println("修改子节点数据:" + new String(event.getData().getData()));
                }
            }
        });*/

        // ==================================================华丽的分割线==================================================
        /*
         * 【为节点和子节点添加watcher】
         * TreeCache可以监听当前节点及其所有子节点的事件。
         */
        /*String treeCachePath = nodePath + "/treeCache";
        final TreeCache treeCache = new TreeCache(cto.client, treeCachePath);
        treeCache.start();
        caches.put(treeCachePath, treeCache);
        
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                System.out.println("-------------treeCache-----------------");
                String type = event.getType().name();
                System.out.println("event type:" + type);
                if (type.equals("NODE_ADDED") || type.equals("NODE_UPDATED") || type.equals("NODE_REMOVED")) {
                    System.out.println("path:" + event.getData().getPath());
                    System.out.println(
                        "data:" + (event.getData().getData() != null ? new String(event.getData().getData()) : ""));
                }
                System.out.println("---------------------------------------");
            }
        });*/

        // ==================================================华丽的分割线==================================================
        /*
        在低版本的curator(4.0.1)中，使用NodeCache、PathChildrenCache、TreeCache进行节点事件的监听。
        在高版本的curator(5.1.0)中，弃用了原来的三种方式：NodeCache 、PathChildrenCache 、TreeCache 。使用新的CuratorCache类进行监听。
        在curator 5.1.0中，NodeCache 、PathChildrenCache 、TreeCache 均被弃用，使用CuratorCache进行所有节点的事件监听，CuratorCache可以监听当前节点及其所有子节点的事件。
        https://blog.csdn.net/duke_ding2/article/details/128345799
        */

        // 【会报错，尚未执行成功：Unable to read additional data from server sessionid 0x30026bc319e0002, likely server has closed
        // socket】
        // curator 5.1.0：NODE_CREATED、NODE_CHANGED、NODE_DELETED
        String curatorCachePath = nodePath + "/curatorCache";
        CuratorCache curatorCache = CuratorCache.build(cto.client, curatorCachePath);
        curatorCache.listenable().addListener(new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                if (type.name().equals(CuratorCacheListener.Type.NODE_CREATED.name())) {
                    // （注意：创建节点时，oldData为null）
                    System.out.println("A new node was added to the cache :" + data.getPath());
                } else if (type.name().equals(CuratorCacheListener.Type.NODE_CHANGED.name())) {
                    System.out.println("A node already in the cache has changed :" + data.getPath());
                } else {
                    // NODE_DELETED： node already in the cache was deleted.（注意：删除节点时，data为null）
                    System.out.println("A node already in the cache was deleted :" + oldData.getPath());
                }
            }
        });
        curatorCache.start();

        Thread.sleep(10000);
        cto.closeZKClient();
    }

}
