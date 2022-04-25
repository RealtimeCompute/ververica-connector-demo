package com.alibaba.ververica.connector.demo;

import com.alibaba.ververica.connectors.hologres.binlog.HologresBinlogConfigs;
import com.alibaba.ververica.connectors.hologres.binlog.HolohubClientProvider;
import com.alibaba.ververica.connectors.hologres.binlog.RetryUtil;
import com.alibaba.ververica.connectors.hologres.binlog.RowDataRecordConverter;
import com.alibaba.ververica.connectors.hologres.binlog.source.HologresBinlogSource;
import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
import shaded.com.aliyun.datahub.client.model.GetTopicResult;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

public class HologresBinlogSourceDemo {

    public static void main(String[] args) throws Exception {

        // 初始化读取的表的schema，需要和holo表schema的字段匹配，可以只定义部分字段
        TableSchema schema = TableSchema.builder()
                .field("a", DataTypes.INT())
                .build();

        // hologres的相关参数，具体参数含义可以参考sql文档
        Configuration config = HologresDemoUtil.createConfiguration();
        config.setBoolean(HologresBinlogConfigs.BINLOG_CDC_MODE, true);

        JDBCOptions jdbcOptions = buildJDBCOption(config);

        RowDataRecordConverter recordConverter = buildRecordConverter(schema, config, jdbcOptions);

        long startTimeMs = 0;

        HologresBinlogSource<RowData> source = new HologresBinlogSource<>(
                schema,
                config,
                jdbcOptions,
                recordConverter,
                startTimeMs);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Test source").print();
        env.execute();
    }

    private static RowDataRecordConverter buildRecordConverter(TableSchema schema, Configuration config, JDBCOptions jdbcOptions) {
        GetTopicResult getTopicResult =
                RetryUtil.getTopicWithRetry(
                        HolohubClientProvider.newHolohubClientProvider(jdbcOptions).getClient(),
                        jdbcOptions.getDatabase(),
                        jdbcOptions.getBinlogTableName());

        return new RowDataRecordConverter(
                jdbcOptions.getTable(),
                schema,
                getTopicResult,
                config.getBoolean(HologresBinlogConfigs.BINLOG_CDC_MODE));
    }

    public static JDBCOptions buildJDBCOption(Configuration config) {
        JDBCOptions jdbcOptions = JDBCUtils.getJDBCOptions(config);
        jdbcOptions.setHolohubEndpoint(JDBCUtils.getHolohubEndpoint(jdbcOptions));
        return jdbcOptions;
    }

}
