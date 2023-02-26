package com.coding.zk.zk01.countdownlatch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CheckStartUp {

    private static List<DangerCenter> stationList;
    private static CountDownLatch countDown;

    public CheckStartUp() {}

    public static boolean checkAllStations() throws Exception {

        // 初始化3个调度站
        countDown = new CountDownLatch(3);

        // 把所有站点添加进list
        stationList = new ArrayList<>();
        stationList.add(new StationBeijing(countDown));
        stationList.add(new StationJiangsuSanling(countDown));
        stationList.add(new StationShandongChangchuan(countDown));

        // 使用线程池
        Executor executor = Executors.newFixedThreadPool(stationList.size());

        for (DangerCenter center : stationList) {
            executor.execute(center);
        }

        // 等待线程执行完毕
        countDown.await();

        // System.out.println(111);
        for (DangerCenter center : stationList) {
            // System.out.println(222);
            if (!center.isOk()) {
                // System.out.println(333);
                return false;
            }
            // System.out.println(444);
        }
        // System.out.println(555);
        long zz = countDown.getCount();
        return true;
    }

    public static void main(String[] args) throws Exception {
        boolean result = CheckStartUp.checkAllStations();
        System.out.println("监控中心针对所有危化品调度站点的检查结果为：" + result);
    }
}
