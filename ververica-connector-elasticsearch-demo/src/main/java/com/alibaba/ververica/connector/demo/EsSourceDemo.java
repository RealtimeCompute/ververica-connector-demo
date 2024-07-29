package com.alibaba.ververica.connector.demo;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class EsSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE elasticsearch_source (\n" +
                        "  name STRING,\n" +
                        "  location STRING,\n" +
                        "  `value` FLOAT\n" +
                        ") WITH (\n" +
                        "  'connector' ='elasticsearch',\n" +
                        "  'endPoint' = '<yourEndPoint>',\n" +
                        "  'accessId' = '${secret_values.ak_id}',\n" +
                        "  'accessKey' = '${secret_values.ak_secret}',\n" +
                        "  'indexName' = '<yourIndexName>',\n" +
                        "  'typeNames' = '<yourTypeName>'\n" +
                        ");"
        );

        tEnv.toDataStream(tEnv.from("es_source")).addSink(new PrintSinkFunction<>());
        env.execute("ES source demo");
    }
}
