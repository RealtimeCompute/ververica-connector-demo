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

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.mongodb.shaded.com.mongodb.client.model.UpdateOneModel;
import org.apache.flink.mongodb.shaded.com.mongodb.client.model.UpdateOptions;
import org.apache.flink.mongodb.shaded.com.mongodb.client.model.WriteModel;
import org.apache.flink.mongodb.shaded.org.bson.BsonDocument;
import org.apache.flink.mongodb.shaded.org.bson.Document;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MongoDBSinkDemo {
    public static void main(String[] args) throws Exception {
        MongoSink<Document> sink = MongoSink.<Document>builder()
                .setUri("mongodb://localhost:27017")
                .setDatabase("my_database")
                .setCollection("my_collection")
                .setBatchSize(5)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setSerializationSchema(new UpsertSerializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.fromSequence(1, 5)
                .map(i -> String.format("{ \"id\": %d }", i))
                .map(Document::parse)
                .sinkTo(sink);
        env.execute("Mongo Sink Demo");
    }

    private static class UpsertSerializationSchema implements MongoSerializationSchema<Document> {
        @Override
        public WriteModel<BsonDocument> serialize(Document element, MongoSinkContext sinkContext) {
            BsonDocument document = element.toBsonDocument();
            BsonDocument filter = new BsonDocument("_id", document.get("_id"));
            document.remove("_id");
            BsonDocument update = new BsonDocument("$set", document);
            return new UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true));
        }
    }
}