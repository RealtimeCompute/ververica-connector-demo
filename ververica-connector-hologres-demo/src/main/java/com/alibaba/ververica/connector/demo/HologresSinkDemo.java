package com.alibaba.ververica.connector.demo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import com.alibaba.ververica.connectors.hologres.api.AbstractHologresWriter;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCWriter;
import com.alibaba.ververica.connectors.hologres.sink.HologresSinkFunction;

public class HologresSinkDemo {
    public static void main(String[] args) throws Exception {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 初始化读取的表的Schema，需要和Hologres表Schema的字段匹配，可以只定义部分字段。
        TableSchema schema =
                TableSchema.builder()
                        .field("a", DataTypes.INT().notNull())
                        .field("b", DataTypes.STRING())
                        .primaryKey("a")
                        .build();

        // Hologres的相关参数。
        Configuration config = HologresDemoUtil.createConfiguration();

        HologresConnectionParam hologresConnectionParam = new HologresConnectionParam(config);
        // 构建Hologres Writer，以RowData的方式写入数据。
        AbstractHologresWriter<RowData> hologresWriter =
                HologresJDBCWriter.createRowDataWriter(
                        hologresConnectionParam,
                        schema,
                        HologresTableSchema.get(hologresConnectionParam),
                        new Integer[0]);
        // 构建Hologres Sink。
        HologresSinkFunction sinkFunction =
                new HologresSinkFunction(hologresConnectionParam, hologresWriter);
        TypeInformation<RowData> typeInfo =
                InternalTypeInfo.of(schema.toRowDataType().getLogicalType());
        int offset = (int) (System.currentTimeMillis() % Integer.MAX_VALUE);
        env.fromElements(
                        (RowData) GenericRowData.of(2 + offset, StringData.fromString("2")),
                        GenericRowData.of(3 + offset, StringData.fromString("3")))
                .returns(typeInfo)
                .addSink(sinkFunction);

        env.execute();
    }
}