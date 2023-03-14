package com.coding.zk.spring.zk01.demo.callback;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;

public class CreateCallBack extends Stat implements AsyncCallback.StringCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        System.out.println("创建节点: " + path);
        System.out.println((String)ctx);
    }
}
