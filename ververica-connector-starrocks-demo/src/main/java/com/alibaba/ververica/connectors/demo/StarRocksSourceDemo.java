package com.alibaba.ververica.connectors.demo;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StarRocksSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE `runoob_tbl_source` (\n" +
                        "  `runoob_id` BIGINT NOT NULL,\n" +
                        "  `runoob_title` STRING NOT NULL,\n" +
                        "  `runoob_author` STRING NOT NULL,\n" +
                        "  `submission_date` DATE NULL\n" +
                        ") WITH (\n" +
                        "  'connector' = 'starrocks',\n" +
                        "  'jdbc-url' = 'jdbc:mysql://ip:9030',\n" +
                        "  'scan-url' = 'ip:18030',\n" +
                        "  'database-name' = 'db_name',\n" +
                        "  'table-name' = 'table_name',\n" +
                        "  'password' = 'xxxxxxx',\n" +
                        "  'username' = 'xxxxx'\n" +
                        ");"
        );

        tEnv.toDataStream(tEnv.from("es_source")).addSink(new PrintSinkFunction<>());
        env.execute("ES source demo");
    }
}
