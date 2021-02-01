package com.alibaba.ververica.connector.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.types.Row;

import com.alibaba.ververica.connectors.common.sink.TupleOutputFormatSinkFunction;
import com.alibaba.ververica.connectors.odps.OdpsOptions;
import com.alibaba.ververica.connectors.odps.sink.OdpsOutputFormat;

import java.util.Arrays;

public class OdpsSinkDemo {
	public static void main(String[] args) throws Exception {
		// ODPS configuration, same as SQL connector.
		DescriptorProperties properties = new DescriptorProperties();
		properties.putString(OdpsOptions.END_POINT.key(), "http://xx.com/api");
		properties.putString(OdpsOptions.PROJECT_NAME.key(), "test");
		properties.putString(OdpsOptions.TABLE_NAME.key(), "test_table");
		properties.putString(OdpsOptions.ACCESS_ID.key(), "xxx");
		properties.putString(OdpsOptions.ACCESS_KEY.key(), "xxxx");
		properties.putString(OdpsOptions.PARTITION.key(), "ds=20201101");

		// meta information for odps columns
		TableSchema schema = TableSchema.builder()
				.field("a", DataTypes.STRING())
				.field("b", DataTypes.STRING())
				.build();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// Note: odps sink commits data when checkpointing to best-effort achieve exactly-once.
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);

		TupleOutputFormatSinkFunction<Row> odpsSink = new TupleOutputFormatSinkFunction<>(
				new OdpsOutputFormat(schema, properties));

		env.fromCollection(
				Arrays.asList(Row.of("aa", "111"), Row.of("bb", "222"))
		).map(new MapFunction<Row, Tuple2<Boolean, Row>>() {
			@Override
			public Tuple2<Boolean, Row> map(Row row) throws Exception {
				return Tuple2.of(true, row);
			}
		}).addSink(odpsSink);

		env.execute("odps sink");
	}
}
