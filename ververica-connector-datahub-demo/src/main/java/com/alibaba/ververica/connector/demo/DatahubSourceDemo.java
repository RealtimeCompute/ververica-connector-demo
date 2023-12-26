package com.alibaba.ververica.connector.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.ververica.connectors.datahub.source.DatahubSourceFunction;
import shaded.datahub.com.aliyun.datahub.client.model.RecordEntry;
import shaded.datahub.com.aliyun.datahub.client.model.TupleRecordData;

import java.io.Serializable;

public class DatahubSourceDemo implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String ENDPOINT = "";
    private static final String PROJECT_NAME = "";
    private static final String TOPIC_NAME = "";
    private static final String SUB_ID = "";
    private static final String ACCESS_ID = "";
    private static final String ACCESS_KEY = "";
    private static final String RUN_MODE = "public";
    private static final boolean ENABLE_SCHEMA_REGISTRY = false;

    public void runExample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DatahubSourceFunction datahubSource =
                new DatahubSourceFunction(
                        ENDPOINT,
                        PROJECT_NAME,
                        TOPIC_NAME,
                        SUB_ID,
                        ACCESS_ID,
                        ACCESS_KEY,
                        RUN_MODE,
                        ENABLE_SCHEMA_REGISTRY,
                        0,
                        1588231223000L);
        datahubSource.setRequestTimeout(30 * 1000);
        datahubSource.enableExitAfterReadFinished();

        env.addSource(datahubSource)
                .map((MapFunction<RecordEntry, Tuple2<String, Long>>) this::getStringLongTuple2)
                .print();
        env.execute();
    }

    private Tuple2<String, Long> getStringLongTuple2(RecordEntry recordEntry) {
        Tuple2<String, Long> tuple2 = new Tuple2<>();
        TupleRecordData recordData = (TupleRecordData) (recordEntry.getRecordData());
        tuple2.f0 = (String) recordData.getField(0);
        tuple2.f1 = (Long) recordData.getField(1);
        return tuple2;
    }

    public static void main(String[] args) throws Exception {
        DatahubSourceDemo sourceFunctionExample = new DatahubSourceDemo();
        sourceFunctionExample.runExample();
    }
}