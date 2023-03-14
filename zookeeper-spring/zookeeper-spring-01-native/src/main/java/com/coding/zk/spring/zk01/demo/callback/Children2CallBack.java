package com.coding.zk.spring.zk01.demo.callback;

import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;

public class Children2CallBack implements AsyncCallback.Children2Callback {

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        for (String s : children) {
            System.out.println(s);
        }
        System.out.println("Children2CallBack:" + path);
        System.out.println((String)ctx);
        System.out.println(stat.toString());
    }

}
