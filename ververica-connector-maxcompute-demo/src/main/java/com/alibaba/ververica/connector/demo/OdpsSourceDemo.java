package com.alibaba.ververica.connector.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OdpsSourceDemo {
	public static void main(String[] args) throws Exception {
		// demo1
		testOdpsSource();
		// demo2
		testContinuousOdpsSource();
	}

	// demo1: odps source
	private static void testOdpsSource() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		tEnv.executeSql(
				String.join(
						"\n",
						"CREATE TABLE odps_source (",
						"  a STRING,",
						"  b STRING",
						") WITH (",
						"  'connector' = 'odps',",
						"  'endPoint' = 'http://xx.com/api',",
						"  'project' = 'test',",
						"  'tableName' = 'test_table',",
						"  'accessId' = 'xxx',",
						"  'accessKey' = 'xxxx',",
						"  'partition' = 'ds=20201101'",
						")"));

		tEnv.toDataStream(tEnv.from("odps_source")).addSink(new PrintSinkFunction<>());
		env.execute("odps source");
	}

	// demo2: continuous odps source
	private static void testContinuousOdpsSource() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		tEnv.executeSql(
				String.join(
						"\n",
						"CREATE TABLE continuous_odps_source (",
						"  a STRING,",
						"  b STRING",
						") WITH (",
						"  'connector' = 'odps',",
						"  'endPoint' = 'http://xx.com/api',",
						"  'project' = 'test',",
						"  'tableName' = 'test_table',",
						"  'accessId' = 'xxx',",
						"  'accessKey' = 'xxxx',",
						"  'startPartition' = 'ds=20201101'",
						")"));

		tEnv.toDataStream(tEnv.from("continuous_odps_source")).addSink(new PrintSinkFunction<>());
		env.execute("continuous odps source");
	}
}