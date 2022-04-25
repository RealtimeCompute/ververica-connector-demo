package com.alibaba.ververica.connector.demo;

import com.alibaba.ververica.connectors.hologres.config.HologresConfigs;
import org.apache.flink.configuration.Configuration;

public class HologresDemoUtil {
    public static Configuration createConfiguration() {
        // hologres的相关参数，具体参数含义可以参考sql文档
        Configuration config = new Configuration();
        config.setString(HologresConfigs.ENDPOINT, "<holo-endpoint>");
        config.setString(HologresConfigs.USERNAME, "<holo-username>");
        config.setString(HologresConfigs.PASSWORD, "<holo-password>");
        config.setString(HologresConfigs.DATABASE, "<holo-database>");
        config.setString(HologresConfigs.TABLE, "<holo-table-name>");
        return config;
    }
}
