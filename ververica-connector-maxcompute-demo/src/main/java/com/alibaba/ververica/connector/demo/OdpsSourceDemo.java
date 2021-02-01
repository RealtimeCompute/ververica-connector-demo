package com.alibaba.ververica.connector.demo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Preconditions;

import com.alibaba.ververica.connectors.continuous.odps.source.ContinuousODPSStreamSource;
import com.alibaba.ververica.connectors.odps.ODPSStreamSource;
import com.alibaba.ververica.connectors.odps.OdpsConf;
import com.alibaba.ververica.connectors.odps.OdpsOptions;
import com.alibaba.ververica.connectors.odps.schema.ODPSColumn;
import com.alibaba.ververica.connectors.odps.schema.ODPSTableSchema;
import com.alibaba.ververica.connectors.odps.util.OdpsUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class OdpsSourceDemo {
	public static void main(String[] args) throws Exception {
		// demo1
		testOdpsSource();
		// demo2
		// testContinuousOdpsSource();
	}

	// demo1: odps source
	private static void testOdpsSource() throws Exception {
		// ODPS configuration, same as SQL connector.
		Configuration conf = new Configuration();
		conf.setString(OdpsOptions.END_POINT.key(), "http://xxx.com/api");
		conf.setString(OdpsOptions.PROJECT_NAME.key(), "test");
		conf.setString(OdpsOptions.TABLE_NAME.key(), "test_table");
		conf.setString(OdpsOptions.ACCESS_ID.key(), "xxx");
		conf.setString(OdpsOptions.ACCESS_KEY.key(), "xxxx");
		conf.setString(OdpsOptions.PARTITION.key(), "ds=20201101");

		// meta information for odps columns
		TableSchema schema = org.apache.flink.table.api.TableSchema.builder()
				.field("a", DataTypes.STRING())
				.field("b", DataTypes.STRING())
				.build();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ODPSStreamSource odpsSource =
				new OdpsSourceBuilder(schema, conf).buildSourceFunction();
		DataStreamSource<RowData> source = env.addSource(odpsSource);
		source.addSink(new PrintSinkFunction<>());
		env.execute("odps source");
	}

	// demo2: continuous odps source
	private static void testContinuousOdpsSource() throws Exception {
		Configuration conf = new Configuration();
		conf.setString(OdpsOptions.END_POINT.key(), "http://xxx.com/api");
		conf.setString(OdpsOptions.PROJECT_NAME.key(), "test");
		conf.setString(OdpsOptions.TABLE_NAME.key(), "test_table");
		conf.setString(OdpsOptions.ACCESS_ID.key(), "xxx");
		conf.setString(OdpsOptions.ACCESS_KEY.key(), "xxxx");
		conf.setString(OdpsOptions.START_PARTITION.key(), "ds=20201101");

		TableSchema schema = org.apache.flink.table.api.TableSchema.builder()
				.field("a", DataTypes.STRING())
				.field("b", DataTypes.STRING())
				.build();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ContinuousODPSStreamSource continuousOdpsSource =
				new OdpsSourceBuilder(schema, conf).buildContinuousOdpsSource();
		DataStreamSource<RowData> source = env.addSource(continuousOdpsSource);
		source.addSink(new PrintSinkFunction<>());
		env.execute("continuous odps source");
	}

	public static class OdpsSourceBuilder {

		private final OdpsConf odpsConf;
		private final String startPartition;
		private final String odpsTable;

		private TypeInformation<RowData> producedTypeInfo;
		private ODPSColumn[] selectedColumns;

		private long retryTimes;
		private long sleepTimesMs;
		private long maxPartitionCount;
		private int discoveryIntervalInMs;
		private List<String> prunedPartitions;

		public OdpsSourceBuilder(
				TableSchema tableSchema,
				Configuration conf) {
			this.odpsConf = OdpsUtils.createOdpsConf(conf);
			this.startPartition = conf.getString(OdpsOptions.START_PARTITION);
			this.odpsTable = conf.getString(OdpsOptions.TABLE_NAME);
			String specificPartition = conf.getString(OdpsOptions.PARTITION);
			ODPSTableSchema physicalTableSchema = OdpsUtils.getOdpsTableSchema(odpsConf, odpsTable);

			boolean isPartitionedTable = physicalTableSchema.isPartition();
			this.maxPartitionCount = conf.getInteger(OdpsOptions.MAX_PARTITION_COUNT);
			this.prunedPartitions = getSpecificPartitions(
					odpsConf, odpsTable, isPartitionedTable, specificPartition);

			Preconditions.checkArgument(
					isPartitionedTable || StringUtils.isEmpty(startPartition),
					"Non-partitioned table can not be an unbounded source.");

			this.producedTypeInfo = InternalTypeInfo.of(tableSchema.toRowDataType().getLogicalType());
			this.selectedColumns = OdpsUtils.validateAndGetProjectCols(physicalTableSchema, tableSchema);

			this.discoveryIntervalInMs = conf.getInteger(OdpsOptions.SUBSCRIBE_INTERVAL_IN_SEC);
			this.retryTimes = conf.getInteger(OdpsOptions.RETRY_TIME);
			this.sleepTimesMs = conf.getInteger(OdpsOptions.SLEEP_MILLIS);
		}

		public ODPSStreamSource buildSourceFunction() {
			return new ODPSStreamSource(
					odpsConf,
					odpsTable,
					selectedColumns,
					prunedPartitions,
					producedTypeInfo,
					sleepTimesMs,
					retryTimes);
		}

		public ContinuousODPSStreamSource buildContinuousOdpsSource() {
			return new ContinuousODPSStreamSource(
					odpsConf,
					odpsTable,
					selectedColumns,
					producedTypeInfo,
					sleepTimesMs,
					retryTimes,
					startPartition,
					discoveryIntervalInMs);
		}

		private List<String> getSpecificPartitions(
				OdpsConf odpsConf,
				String odpsTable,
				boolean isPartitionedTable,
				String specificPartition) {
			if (!isPartitionedTable) {
				return new ArrayList<>();
			}
			List<String> conditions = new ArrayList<>();
			if (StringUtils.isNotEmpty(specificPartition)) {
				conditions.add(specificPartition);
			}
			List<String> partitions = OdpsUtils.getMatchedPartitions(
					odpsConf, odpsTable, conditions, true, true);

			if (partitions.size() > maxPartitionCount) {
				throw new TableException(
						String.format(
								"The number of matched partitions [%d] exceeds"
										+ " the default limit of [%d]! \nPlease confirm whether you need to read all these "
										+ "partitions, if you really need it, you can increase the `maxPartitionCount` "
										+ "in DDL's with options.",
								partitions.size(), maxPartitionCount));
			}
			return partitions;
		}
	}
}
