package com.coding.zk.zk01.demo.callback;

import org.apache.zookeeper.AsyncCallback;

public class DeleteCallBack implements AsyncCallback.VoidCallback {
    @Override
    public void processResult(int rc, String path, Object ctx) {
        System.out.println("删除节点" + path);
        System.out.println((String)ctx);
    }
}
