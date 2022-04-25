package com.alibaba.ververica.connector.demo;

import com.alibaba.ververica.connectors.hologres.api.AbstractHologresWriter;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.config.HologresConfigs;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCWriter;
import com.alibaba.ververica.connectors.hologres.rpc.HologresRpcWriter;
import com.alibaba.ververica.connectors.hologres.sink.HologresSinkFunction;
import com.alibaba.ververica.connectors.hologres.utils.HologresUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

public class HologresSinkDemo {
    public static void main(String[] args) throws Exception {
        TableSchema schema = TableSchema.builder()
                .field("a", DataTypes.INT())
                .field("b", DataTypes.STRING())
                .build();

        Configuration config = HologresDemoUtil.createConfiguration();
        config.setBoolean(HologresConfigs.USE_RPC_MODE, true);

        HologresConnectionParam hologresConnectionParam = new HologresConnectionParam(config);
        AbstractHologresWriter<RowData> hologresWriter = buildHologresWriter(schema, config, hologresConnectionParam);

        HologresSinkFunction sinkFunction = new HologresSinkFunction(hologresConnectionParam, hologresWriter);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TypeInformation<RowData> typeInfo = InternalTypeInfo.of(schema.toRowDataType().getLogicalType());
        env.fromElements((RowData)GenericRowData.of(2, StringData.fromString("2")), GenericRowData.of(3, StringData.fromString("3"))).returns(typeInfo)
            .addSink(sinkFunction);

        env.execute();
    }

    private static AbstractHologresWriter<RowData> buildHologresWriter(TableSchema schema, Configuration config, HologresConnectionParam hologresConnectionParam) {
        HologresTableSchema hologresTableSchema = HologresTableSchema.get(hologresConnectionParam);
        AbstractHologresWriter<RowData> hologresWriter;
        if (HologresUtils.shouldUseRpc(config)) {
            hologresWriter =
                    HologresRpcWriter.createTableWriter(
                            hologresConnectionParam, schema, hologresTableSchema);
        } else {
            hologresWriter =
                    HologresJDBCWriter.createTableWriter(
                            hologresConnectionParam, schema, hologresTableSchema);
        }
        return hologresWriter;
    }

}
