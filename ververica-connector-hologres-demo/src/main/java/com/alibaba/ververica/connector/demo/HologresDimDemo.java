package com.alibaba.ververica.connector.demo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import com.alibaba.ververica.connectors.common.dim.cache.CacheConfig;
import com.alibaba.ververica.connectors.hologres.api.AbstractHologresReader;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCReader;
import com.alibaba.ververica.connectors.hologres.source.lookup.HologresAsyncLookupFunction;
import com.alibaba.ververica.connectors.hologres.source.lookup.HologresLookupFunction;

import java.util.concurrent.TimeUnit;

public class HologresDimDemo {
    public static void main(String[] args) throws Exception {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 初始化源表的Schema。
        TableSchema schema =
                TableSchema.builder()
                        .field("a", DataTypes.INT())
                        .field("b", DataTypes.STRING())
                        .build();

        TypeInformation<RowData> typeInfo =
                InternalTypeInfo.of(schema.toRowDataType().getLogicalType());
        // 源表数据
        DataStream<RowData> stream =
                env.fromElements(
                                (RowData) GenericRowData.of(1, StringData.fromString("foo")),
                                GenericRowData.of(2, StringData.fromString("bar")))
                        .returns(typeInfo);
        // hologres维表建表DDL: create table dim_table (a int primary key, c bigint);
        // 插入数据: insert into dim_table values(1, 123456),(2, 456789);
        // 初始化维表的Schema，需要和Hologres表Schema的字段匹配，可以只定义部分字段，但必须包含主键。
        TableSchema dimSchema =
                TableSchema.builder()
                        .field("a", DataTypes.INT().notNull())
                        .field("c", DataTypes.BIGINT())
                        .primaryKey("a")
                        .build();

        // hologres的相关参数，具体参数含义可以参考连接器文档
        Configuration config = HologresDemoUtil.createConfiguration();
        HologresConnectionParam hologresConnectionParam = new HologresConnectionParam(config);

        // 构建Hologres lookup function，可以选择同步或异步维表查询。
        HologresAsyncLookupFunction hologresAsyncLookupFunction =
                buildHologresAsyncLookUpFunction(dimSchema, config, hologresConnectionParam);
        // 这里使用异步维表关联
        DataStream<RowData> resultStream =
                AsyncDataStream.unorderedWait(
                        stream, hologresAsyncLookupFunction, 10000, TimeUnit.MILLISECONDS);
        resultStream.print();

        // 也可以选择使用同步接口
        // HologresLookupFunction hologresLookupFunction = buildHologresLookUpFunction(dimSchema,
        // config, hologresConnectionParam);
        // stream.flatMap(hologresLookupFunction).print();

        env.execute();
    }

    private static HologresAsyncLookupFunction buildHologresAsyncLookUpFunction(
            TableSchema schema,
            Configuration config,
            HologresConnectionParam hologresConnectionParam) {
        HologresTableSchema hologresTableSchema = HologresTableSchema.get(hologresConnectionParam);
        AbstractHologresReader<RowData> hologresReader;
        // 缓存策略，具体参数含义可以参考文档
        CacheConfig cacheConfig = CacheConfig.createCacheConfig(config, "NONE");
        // 维表与源表lookup的key，必须与物理维表的主键完全一致
        String[] lookupKeys = hologresTableSchema.get().getPrimaryKeys();
        hologresReader =
                HologresJDBCReader.createTableReader(
                        hologresConnectionParam, schema, lookupKeys, hologresTableSchema);
        // 可以根据需要使用 HologresAsyncLookupFunction 或者 HologresLookupFunction
        return new HologresAsyncLookupFunction(
                "dim_table",
                schema,
                lookupKeys,
                cacheConfig.getCacheStrategy(),
                hologresReader,
                true);
    }

    private static HologresLookupFunction buildHologresLookUpFunction(
            TableSchema schema,
            Configuration config,
            HologresConnectionParam hologresConnectionParam) {
        HologresTableSchema hologresTableSchema = HologresTableSchema.get(hologresConnectionParam);
        AbstractHologresReader<RowData> hologresReader;
        // 缓存策略，具体参数含义可以参考sql文档
        CacheConfig cacheConfig = CacheConfig.createCacheConfig(config, "NONE");
        // 维表与源表loopup的key，必须与物理维表的主键完全一致
        String[] lookupKeys = hologresTableSchema.get().getPrimaryKeys();
        hologresReader =
                HologresJDBCReader.createTableReader(
                        hologresConnectionParam, schema, lookupKeys, hologresTableSchema);
        // 可以根据需要使用 HologresAsyncLookupFunction 或者 HologresLookupFunction
        return new HologresLookupFunction(
                "dim_table",
                schema,
                lookupKeys,
                cacheConfig.getCacheStrategy(),
                hologresReader,
                true);
    }
}