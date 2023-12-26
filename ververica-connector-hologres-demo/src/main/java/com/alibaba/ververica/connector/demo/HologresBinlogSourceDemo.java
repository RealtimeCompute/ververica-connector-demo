package com.alibaba.ververica.connector.demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.ververica.connectors.hologres.binlog.HologresBinlogConfigs;
import com.alibaba.ververica.connectors.hologres.binlog.StartupMode;
import com.alibaba.ververica.connectors.hologres.binlog.source.converter.JDBCBinlogRecordConverter;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCBinlogSource;
import com.alibaba.ververica.connectors.hologres.utils.HoloBinlogUtil;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;

import java.util.Collections;

public class HologresBinlogSourceDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 初始化读取的表的Schema，需要和Hologres表Schema的字段匹配，可以只定义部分字段。
        TableSchema schema =
                TableSchema.builder().field("a", DataTypes.INT().notNull()).primaryKey("a").build();

        // Hologres的相关参数。
        Configuration config = HologresDemoUtil.createConfiguration();
        config.setBoolean(HologresBinlogConfigs.OPTIONAL_BINLOG, true);
        config.setBoolean(HologresBinlogConfigs.BINLOG_CDC_MODE, true);
        // 构建JDBC Options。
        JDBCOptions jdbcOptions = JDBCUtils.getJDBCOptions(config);
        // 设置或创建默认slotname, jdbcBinlogSlotName是jdbc模式消费binlog的一个可选参数，
        // 如果不设置，Hologres连接器会创建默认的slot并使用，默认创建的publication名称类似publication_for_table_<table_name>_used_by_flink，
        // 默认创建的slot名称类似slot_for_table_<table_name>_used_by_flink，在使用中如果发生异常，可以尝试删除并重试。
        // 默认创建slot需要一定的前提条件，要求用户为实例的Superuser，或用户同时拥有目标表的Owner权限和实例的Replication Role权限.
        config.setString(
                HologresBinlogConfigs.JDBC_BINLOG_SLOT_NAME,
                HoloBinlogUtil.getOrCreateDefaultSlotForJDBCBinlog(jdbcOptions));

        boolean cdcMode =
                config.get(HologresBinlogConfigs.BINLOG_CDC_MODE)
                        && config.get(HologresBinlogConfigs.OPTIONAL_BINLOG);
        // 构建Binlog Record Converter。
        JDBCBinlogRecordConverter recordConverter =
                new JDBCBinlogRecordConverter(
                        jdbcOptions.getTable(),
                        schema,
                        new HologresConnectionParam(config),
                        cdcMode,
                        Collections.emptySet());

        // 构建Hologres Binlog Source。
        long startTimeMs = 0;
        HologresJDBCBinlogSource source =
                new HologresJDBCBinlogSource(
                        new HologresConnectionParam(config),
                        schema,
                        config,
                        jdbcOptions,
                        startTimeMs,
                        StartupMode.INITIAL,
                        recordConverter,
                        "",
                        -1);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Test source").print();
        env.execute();
    }
}