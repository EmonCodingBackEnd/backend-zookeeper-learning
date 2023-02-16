package com.coding.zk.zk01.countdownlatch;

import java.util.concurrent.CountDownLatch;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StationBeijing extends DangerCenter {
    public StationBeijing(CountDownLatch countDown) {
        super(countDown, "北京调度站");
    }

    @Override
    public void check() {
        log.warn("正在检查 [" + this.getStation() + "]...");

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        log.warn("检查 [" + this.getStation() + "] 完毕，可以发车~");
    }
}
