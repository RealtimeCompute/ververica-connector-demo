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

package com.alibaba.ververica.connector.demo.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.StringSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Locale;
import java.util.Properties;

/**
 * A sample Flink job for consuming messages from Kafka, convert all characters to upper case, then
 * sink into another Kafka topic.
 */
public class KafkaDemoJob {

    public static void main(String[] args) throws Exception {

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Required configurations for connecting to Kafka
        String bootstrapServers = "localhost:9092";
        String inputTopic = "input-topic";
        String outputTopic = "output-topic";
        String groupId = "my-excellent-group";

        // DataStream Source
        DataStreamSource<String> source;

        // Build Kafka properties
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Build Kafka source
        KafkaSource<String> kafkaSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setTopics(inputTopic)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setGroupId(groupId)
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(
                                        StringDeserializer.class))
                        .build();
        source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Build Kafka sink
        KafkaSink<String> kafkaSink =
                KafkaSink.<String>builder()
                        .setKafkaProducerConfig(kafkaProperties)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(outputTopic)
                                        .setKafkaValueSerializer(StringSerializer.class)
                                        .build())
                        .build();

        // Append your transformations after the source
        source
                // Convert all characters to upper case
                .map((message) -> message.toUpperCase(Locale.ROOT))
                .name("Convert to upper case")
                // then sink to Kafka
                .sinkTo(kafkaSink)
                .name("Kafka Sink");

        // Compile and submit the job
        env.execute();
    }
}