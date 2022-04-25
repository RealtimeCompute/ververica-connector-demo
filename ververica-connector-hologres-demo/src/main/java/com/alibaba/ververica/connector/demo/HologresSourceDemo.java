package com.alibaba.ververica.connector.demo;

import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.source.scan.bulkread.HologresBulkreadInputFormat;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

public class HologresSourceDemo {
    public static void main(String[] args) throws Exception {

        TableSchema schema = TableSchema.builder()
                .field("a", DataTypes.INT())
                .build();

        Configuration config = HologresDemoUtil.createConfiguration();

        JDBCOptions jdbcOptions = JDBCUtils.getJDBCOptions(config);

        String query =
                JDBCUtils.getSimpleSelectFromStatement(
                        jdbcOptions.getTable(), schema.getFieldNames());
        HologresBulkreadInputFormat inputFormat = new HologresBulkreadInputFormat(jdbcOptions, schema, query);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TypeInformation<RowData> typeInfo = InternalTypeInfo.of(schema.toRowDataType().getLogicalType());
        env.addSource(new InputFormatSourceFunction<>(inputFormat, typeInfo)).returns(typeInfo)
                .print();
        env.execute();
    }
}


