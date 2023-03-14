package com.coding.zk.spring.zk02.curator.watcher;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;

public class MyCuratorWatcher implements CuratorWatcher {

	@Override
	public void process(WatchedEvent event) throws Exception {
		System.out.println("触发watcher，节点路径为：" + event.getPath());
	}

}
