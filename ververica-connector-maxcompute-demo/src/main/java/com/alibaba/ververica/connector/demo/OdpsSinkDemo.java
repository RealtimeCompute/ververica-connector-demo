package com.alibaba.ververica.connector.demo;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class OdpsSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Note: odps sink commits data when checkpointing to best-effort achieve exactly-once.
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                String.join(
                        "\n",
                        "CREATE TABLE odps_sink (",
                        "  a STRING,",
                        "  b STRING",
                        ") WITH (",
                        "  'connector' = 'odps',",
                        "  'endPoint' = 'http://xx.com/api',",
                        "  'project' = 'test',",
                        "  'tableName' = 'test_table',",
                        "  'accessId' = 'xxx',",
                        "  'accessKey' = 'xxxx',",
                        "  'partition' = 'ds=20201101'",
                        ")"));

        DataStream<Row> stream = env.fromElements(Row.of("aa", "111"), Row.of("bb", "222"));
        TableResult result = tEnv.fromDataStream(stream).insertInto("odps_sink").execute();

        // await() is for mini-cluster running (e.g. IDE) only
        // If submit to cluster, the following line should be commented out
        result.await();
    }
}