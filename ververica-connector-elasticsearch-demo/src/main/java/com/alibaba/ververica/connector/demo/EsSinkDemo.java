package com.alibaba.ververica.connector.demo;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class EsSinkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                String.join(
                        "CREATE TABLE es_sink (\n" +
                                "  user_id STRING,\n" +
                                "  user_name STRING,\n" +
                                "  PRIMARY KEY (user_id) NOT ENFORCED\n" + // 主键可选，如果定义了主键，则作为文档ID，否则文档ID将为随机值。
                                ") WITH (\n" +
                                "  'connector' = 'elasticsearch-6',\n" +
                                "  'hosts' = '<yourHosts>',\n" +
                                "  'index' = '<yourIndex>',\n" +
                                "  'document-type' = '<yourElasticsearch.types>',\n" +
                                "  'username' ='${secret_values.ak_id}',\n" +
                                "  'password' ='${secret_values.ak_secret}'\n" +
                                ");"
                ));

        DataStream<Row> stream = env.fromElements(Row.of("111", "Alice"), Row.of("222", "Bob"));
        TableResult result = tEnv.fromDataStream(stream).insertInto("odps_sink").execute();

        // await() is for mini-cluster running (e.g. IDE) only
        // If submit to cluster, the following line should be commented out
        result.await();
    }
}
