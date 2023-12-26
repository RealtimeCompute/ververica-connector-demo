package com.alibaba.ververica.connector.demo;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.ververica.connector.mq.shaded.com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.ververica.connector.mq.shaded.com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.ververica.connector.mq.shaded.com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.ververica.connector.mq.shaded.com.taobao.metaq.client.MetaPullConsumer;
import com.alibaba.ververica.connectors.common.sink.OutputFormatSinkFunction;
import com.alibaba.ververica.connectors.metaq.MetaQConnect;
import com.alibaba.ververica.connectors.metaq.sink.MetaQOutputFormat;
import com.alibaba.ververica.connectors.metaq.source.legacy.MetaQSourceFunction;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.alibaba.ververica.connector.mq.shaded.com.taobao.metaq.client.ExternConst.NAMESRV_ADDR;
import static com.alibaba.ververica.connector.mq.shaded.com.taobao.metaq.client.ExternConst.PROPERTY_ACCESSKEY;
import static com.alibaba.ververica.connector.mq.shaded.com.taobao.metaq.client.ExternConst.PROPERTY_INSTANCE_ID;
import static com.alibaba.ververica.connector.mq.shaded.com.taobao.metaq.client.ExternConst.PROPERTY_ONS_CHANNEL;
import static com.alibaba.ververica.connector.mq.shaded.com.taobao.metaq.client.ExternConst.PROPERTY_ROCKET_AUTH_ENABLED;
import static com.alibaba.ververica.connector.mq.shaded.com.taobao.metaq.client.ExternConst.PROPERTY_SECRETKEY;
import static com.alibaba.ververica.connectors.metaq.MetaQConnect.createConsumerInstance;

/**
 * A {@link DataStream} demo that illustrates how to consume messages from RocketMQ, convert
 * messages, then produce messages to RocketMQ.
 *
 * <pre>
 * Arguments
 * mqSourceTopic: The consumer topic of the RocketMQ source.
 * mqSourceConsumerGroup: The consumer group of the RocketMQ source.
 * mqSourceEndpoint: The endpoint address of consumer topic for the RocketMQ source.
 * mqSourceAccessId: The access id of consumer topic for the RocketMQ source.
 * mqSourceAccessKey: The access key of consumer topic for the RocketMQ source.
 * mqSourceInstanceId: The instance id of consumer topic for the RocketMQ source.
 * startMessageOffset: The starting offset of message consumption for the RocketMQ source.
 * startTime: The starting time of message consumption for the RocketMQ source.
 * mqSinkTopic: The producer topic of the RocketMQ sink.
 * mqSinkProducerGroup: The producer group of the RocketMQ sink.
 * mqSinkEndpoint: The endpoint address of producer topic for the RocketMQ sink.
 * mqSinkAccessId: The access id of producer topic for the RocketMQ sink.
 * mqSinkAccessKey: The access key of producer topic for the RocketMQ sink.
 * mqSinkInstanceId: The instance id of producer topic for the RocketMQ sink.
 * </pre>
 */
public class RocketMQDataStreamDemo {

    public static void main(String[] args) throws Exception {
        // Sets up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool parameters = ParameterTool.fromArgs(args);
        // Creates and adds RocketMQ source.
        env.addSource(createRocketMQSource(parameters))
                // Converts message body to upper case.
                .map(RocketMQDataStreamDemo::convertMessages)
                // Creates and adds RocketMQ sink.
                .addSink(new OutputFormatSinkFunction<>(createRocketMQOutputFormat(parameters)))
                .name(RocketMQDataStreamDemo.class.getSimpleName());
        // Compiles and submits job.
        env.execute("RocketMQ connector end-to-end DataStream demo");
    }

    private static MetaQSourceFunction createRocketMQSource(ParameterTool parameters) {
        String sourceTopic = parameters.get("mqSourceTopic");
        String consumerGroup = parameters.get("mqSourceConsumerGroup");
        Properties mqProperties = createSourceMQProperties(parameters);
        int partitionCount = 0;
        MetaPullConsumer consumer = null;
        try {
            consumer = createConsumerInstance(sourceTopic, consumerGroup, mqProperties, null, -1);
            Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(sourceTopic);
            partitionCount = queues == null ? 0 : queues.size();
        } catch (MQClientException e) {
            throw new RuntimeException(
                    "Fetches RocketMQ partition count for RocketMQ source exception", e);
        } finally {
            if (consumer != null) {
                try {
                    MetaQConnect.shutdownConsumer(consumer);
                } catch (Exception ignored) {
                }
            }
        }
        return new MetaQSourceFunction(
                sourceTopic,
                consumerGroup,
                null,
                null,
                100,
                partitionCount,
                Long.MAX_VALUE,
                Long.parseLong(parameters.get("startMessageOffset")),
                Long.parseLong(parameters.get("startTime")),
                mqProperties);
    }

    private static MetaQOutputFormat createRocketMQOutputFormat(ParameterTool parameters) {
        return new MetaQOutputFormat.Builder()
                .setTopicName(parameters.get("mqSinkTopic"))
                .setProducerGroup(parameters.get("mqSinkProducerGroup"))
                .setMqProperties(createSinkMQProperties(parameters))
                .build();
    }

    private static Properties createSourceMQProperties(ParameterTool parameters) {
        Properties properties = new Properties();
        properties.put(PROPERTY_ONS_CHANNEL, "ALIYUN");
        properties.put(NAMESRV_ADDR, parameters.get("mqSourceEndpoint"));
        properties.put(PROPERTY_ACCESSKEY, parameters.get("mqSourceAccessId"));
        properties.put(PROPERTY_SECRETKEY, parameters.get("mqSourceAccessKey"));
        properties.put(PROPERTY_ROCKET_AUTH_ENABLED, true);
        properties.put(PROPERTY_INSTANCE_ID, parameters.get("mqSourceInstanceId"));
        return properties;
    }

    private static Properties createSinkMQProperties(ParameterTool parameters) {
        Properties properties = new Properties();
        properties.put(PROPERTY_ONS_CHANNEL, "ALIYUN");
        properties.put(NAMESRV_ADDR, parameters.get("mqSinkEndpoint"));
        properties.put(PROPERTY_ACCESSKEY, parameters.get("mqSinkAccessId"));
        properties.put(PROPERTY_SECRETKEY, parameters.get("mqSinkAccessKey"));
        properties.put(PROPERTY_ROCKET_AUTH_ENABLED, true);
        properties.put(PROPERTY_INSTANCE_ID, parameters.get("mqSinkInstanceId"));
        return properties;
    }

    private static List<MessageExt> convertMessages(List<MessageExt> messages) {
        messages.forEach(
                message -> message.setBody(new String(message.getBody()).toUpperCase().getBytes()));
        return messages;
    }
}