package com.coding.zk.spring.zk02.utils;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RedisConfig {

    private String type; // add 新增配置 update 更新配置 delete 删除配置
    private String url; // 如果是add或update，则提供下载地址
    private String remark; // 备注

}
