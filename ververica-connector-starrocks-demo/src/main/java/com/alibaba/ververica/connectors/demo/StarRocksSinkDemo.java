package com.alibaba.ververica.connectors.demo;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.concurrent.ExecutionException;

public class StarRocksSinkDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                String.join(
                        "CREATE TABLE `runoob_tbl_sink` (\n" +
                                "  `runoob_id` BIGINT NOT NULL,\n" +
                                "  `runoob_title` STRING NOT NULL,\n" +
                                "  `runoob_author` STRING NOT NULL,\n" +
                                "  `submission_date` DATE NULL\n" +
                                "  PRIMARY KEY(`runoob_id`)\n" +
                                "  NOT ENFORCED\n" +
                                ") WITH (\n" +
                                "  'jdbc-url' = 'jdbc:mysql://ip:9030',\n" +
                                "  'connector' = 'starrocks',\n" +
                                "  'load-url' = 'ip:18030',\n" +
                                "  'database-name' = 'db_name',\n" +
                                "  'table-name' = 'table_name',\n" +
                                "  'password' = 'xxxxxxx',\n" +
                                "  'username' = 'xxxx',\n" +
                                "  'sink.buffer-flush.interval-ms' = '5000'\n" +
                                ");"
                ));

        DataStream<Row> stream = env.fromElements(
                Row.of(1, "111", "Alice", "AAA"),
                Row.of(2, "222", "Bob", "BBB")
        );
        TableResult result = tEnv.fromDataStream(stream).insertInto("odps_sink").execute();

        // await() is for mini-cluster running (e.g. IDE) only
        // If submit to cluster, the following line should be commented out
        result.await();
    }
}
