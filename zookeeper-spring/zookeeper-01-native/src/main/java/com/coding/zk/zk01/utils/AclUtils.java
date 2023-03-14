package com.coding.zk.zk01.utils;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

public class AclUtils {
    public static String getDigestUserPwd(String id) throws Exception {
        return DigestAuthenticationProvider.generateDigest(id);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException, Exception {
        // emon0:VfLYllQszSu5jTPKo9hR8++hZvo=
        /*
        zookeeper的超级管理员 emon:emon123    ==> emon:6/mDgySlNwggKl0eNhUYm7rPFYs=
         */
        String id = "emon:emon123";
        String idDigested = getDigestUserPwd(id);
        System.out.println(idDigested);
    }
}
