package com.coding.zk.spring.zk01.demo.callback;

import java.util.List;

import org.apache.zookeeper.AsyncCallback;

public class ChildrenCallBack implements AsyncCallback.ChildrenCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
        for (String s : children) {
            System.out.println(s);
        }
        System.out.println("ChildrenCallback:" + path);
        System.out.println((String)ctx);
    }
}
