package com.alibaba.ververica.connector.demo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.util.HashMap;
import java.util.Map;

public class CdcDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String dbName = "test_db";
        String tableName = "test_tbl";
        String basePath = "oss://xxx";

        Map<String, String> options = new HashMap<>();
        // hudi conf
        options.put(FlinkOptions.PATH.key(), basePath);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");
        options.put(FlinkOptions.DATABASE_NAME.key(), dbName);
        options.put(FlinkOptions.TABLE_NAME.key(), tableName);
        // oss conf
        options.put("hadoop.fs.oss.accessKeyId", "xxx");
        options.put("hadoop.fs.oss.accessKeySecret", "xxx");
        // 本地调试使用公网网端，例如oss-cn-hangzhou.aliyuncs.com；提交集群使用内网网端，例如oss-cn-hangzhou-internal.aliyuncs.com
        options.put("hadoop.fs.oss.endpoint", "xxx");
        options.put("hadoop.fs.AbstractFileSystem.oss.impl", "org.apache.hadoop.fs.aliyun.oss.OSS");
        options.put("hadoop.fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
        // dlf conf
        options.put(FlinkOptions.HIVE_SYNC_ENABLED.key(), "true"); // 可选择是否同步DLF
        options.put(FlinkOptions.HIVE_SYNC_MODE.key(), "hms");
        options.put(FlinkOptions.HIVE_SYNC_DB.key(), dbName);
        options.put(FlinkOptions.HIVE_SYNC_TABLE.key(), tableName);
        options.put("hadoop.dlf.catalog.id", "xxx");
        options.put("hadoop.dlf.catalog.accessKeyId", "xxx");
        options.put("hadoop.dlf.catalog.accessKeySecret", "xxx");
        options.put("hadoop.dlf.catalog.region", "xxx");
        //  本地调试使用公网网端，例如dlf.cn-hangzhou.aliyuncs.com，提交集群使用内网网端，例如dlf-vpc.cn-hangzhou.aliyuncs.com
        options.put("hadoop.dlf.catalog.endpoint", "xxx");
        options.put("hadoop.hive.imetastoreclient.factory.class", "com.aliyun.datalake.metastore.hive2.DlfMetaStoreClientFactory");

        DataStream<RowData> dataStream = env.fromElements(
                GenericRowData.of(StringData.fromString("id1"), StringData.fromString("name1"), 22,
                        StringData.fromString("1001"), StringData.fromString("p1")),
                GenericRowData.of(StringData.fromString("id2"), StringData.fromString("name2"), 32,
                        StringData.fromString("1002"), StringData.fromString("p2"))
        );

        HoodiePipeline.Builder builder = HoodiePipeline.builder(tableName)
                .column("uuid string")
                .column("name string")
                .column("age int")
                .column("ts string")
                .column("`partition` string")
                .pk("uuid")
                .partition("partition")
                .options(options);

        builder.sink(dataStream, false); // The second parameter indicating whether the input data stream is bounded
        env.execute("Flink_Hudi_Quick_Start");
    }
}