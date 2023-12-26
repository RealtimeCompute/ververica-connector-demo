package com.alibaba.ververica.connector.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.ververica.connectors.datahub.sink.DatahubRecordConverter;
import com.alibaba.ververica.connectors.datahub.sink.DatahubSinkFunction;
import shaded.datahub.com.aliyun.datahub.client.model.Field;
import shaded.datahub.com.aliyun.datahub.client.model.FieldType;
import shaded.datahub.com.aliyun.datahub.client.model.RecordEntry;
import shaded.datahub.com.aliyun.datahub.client.model.RecordSchema;
import shaded.datahub.com.aliyun.datahub.client.model.TupleRecordData;

import java.io.Serializable;

public class DatahubSinkDemo implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String ENDPOINT = "";
    private static final String PROJECT_NAME = "";
    private static final String TOPIC_NAME = "";
    private static final String ACCESS_ID = "";
    private static final String ACCESS_KEY = "";
    private static final String RUN_MODE = "public";
    private static final boolean ENABLE_SCHEMA_REGISTRY = false;
    private static final int SCHEMA_VERSION = -1;

    public void useDefaultRecordConverter() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.generateSequence(0, 100)
                .map((MapFunction<Long, RecordEntry>) aLong -> getRecordEntry(aLong, "default:"))
                .addSink(
                        new DatahubSinkFunction<>(
                                ENDPOINT,
                                PROJECT_NAME,
                                TOPIC_NAME,
                                ACCESS_ID,
                                ACCESS_KEY,
                                RUN_MODE,
                                ENABLE_SCHEMA_REGISTRY,
                                SCHEMA_VERSION));
        env.execute();
    }

    public void useCustomRecordConverter() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DatahubRecordConverter<Long> recordConverter =
                message -> getRecordEntry(message, "custom:");
        env.generateSequence(0, 100)
                .addSink(
                        new DatahubSinkFunction<>(
                                ENDPOINT,
                                PROJECT_NAME,
                                TOPIC_NAME,
                                ACCESS_ID,
                                ACCESS_KEY,
                                RUN_MODE,
                                ENABLE_SCHEMA_REGISTRY,
                                SCHEMA_VERSION,
                                recordConverter));
        env.execute();
    }

    private RecordEntry getRecordEntry(Long message, String s) {
        RecordSchema recordSchema = new RecordSchema();
        recordSchema.addField(new Field("f1", FieldType.STRING));
        recordSchema.addField(new Field("f2", FieldType.BIGINT));
        recordSchema.addField(new Field("f3", FieldType.DOUBLE));
        recordSchema.addField(new Field("f4", FieldType.BOOLEAN));
        recordSchema.addField(new Field("f5", FieldType.TIMESTAMP));
        recordSchema.addField(new Field("f6", FieldType.DECIMAL));
        RecordEntry recordEntry = new RecordEntry();
        TupleRecordData recordData = new TupleRecordData(recordSchema);
        recordData.setField(0, s + message);
        recordData.setField(1, message);
        recordEntry.setRecordData(recordData);
        return recordEntry;
    }

    public static void main(String[] args) throws Exception {
        DatahubSinkDemo sinkFunctionExample = new DatahubSinkDemo();
        sinkFunctionExample.useDefaultRecordConverter();
        sinkFunctionExample.useCustomRecordConverter();
    }
}