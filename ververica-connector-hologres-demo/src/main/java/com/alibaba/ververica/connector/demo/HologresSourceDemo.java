package com.alibaba.ververica.connector.demo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.source.scan.bulkread.HologresBulkreadInputFormat;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;

public class HologresSourceDemo {
    public static void main(String[] args) throws Exception {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 初始化读取的表的Schema，需要和Hologres表Schema的字段匹配，可以只定义部分字段。
        TableSchema schema =
                TableSchema.builder().field("a", DataTypes.INT().notNull()).primaryKey("a").build();

        // Hologres的相关参数。
        Configuration config = HologresDemoUtil.createConfiguration();

        // 构建JDBC Options。
        JDBCOptions jdbcOptions = JDBCUtils.getJDBCOptions(config);

        HologresBulkreadInputFormat inputFormat =
                new HologresBulkreadInputFormat(
                        new HologresConnectionParam(config), jdbcOptions, schema, "", -1);
        TypeInformation<RowData> typeInfo =
                InternalTypeInfo.of(schema.toRowDataType().getLogicalType());
        env.addSource(new InputFormatSourceFunction<>(inputFormat, typeInfo))
                .returns(typeInfo)
                .print();
        env.execute();
    }
}