package com.alibaba.ververica.connector.demo;

import org.apache.flink.configuration.Configuration;

import com.alibaba.ververica.connectors.hologres.config.HologresConfigs;

public class HologresDemoUtil {
    public static Configuration createConfiguration() {
        // hologres的相关参数，具体参数含义可以参考sql文档
        Configuration config = new Configuration();
        config.setString(HologresConfigs.ENDPOINT, "yourEndpoint");
        config.setString(HologresConfigs.USERNAME, "yourUserName");
        config.setString(HologresConfigs.PASSWORD, "yourPassword");
        config.setString(HologresConfigs.DATABASE, "yourDatabaseName");
        config.setString(HologresConfigs.TABLE, "yourTableName");
        config.setString(HologresConfigs.SDK_MODE, "jdbc");
        return config;
    }
}