/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MongoDBSourceDemo {
    public static void main(String[] args) throws Exception {
        MongoDBSource<String> mySqlSource =
                MongoDBSource.<String>builder()
                        .hosts("localhost:27017")
                        .databaseList("db") // set captured database
                        .collectionList("db.coll") // set captured collection
                        .username("username")
                        .password("password")
                        // converts SourceRecord to JSON String
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Mongo Source")
                // set 4 parallel source tasks
                .setParallelism(2)
                .print()
                .setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print Mongo Snapshot + Binlog");
    }
}