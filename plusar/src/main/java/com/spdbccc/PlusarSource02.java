package com.spdbccc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * ClassName: plusar
 * Package: com.spdbccc
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/8 18:48
 * @Version 1.0
 */
public class PlusarSource02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//
//        1. `setServiceUrl("serviceUrl")`：设置连接到Pulsar集群的服务URL。
//        2. `setAdminUrl("adminUrl")`：设置连接到Pulsar集群的管理URL。
//        3. `setStartCursor(StartCursor.earliest())`：设置从最早的可用消息开始读取数据。
//        4. `setTopics("my-topic")`：设置要从中读取数据的Pulsar主题。
//        5. `setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))`：设置用于将Pulsar消息反序列化为Flink数据流的反序列化模式。在这种情况下，使用SimpleStringSchema将消息解析为字符串。
//        6. `setSubscriptionName("my-subscription")`：设置订阅名称，用于在Pulsar中创建唯一的订阅。
//        7. `setSubscriptionType(SubscriptionType.Exclusive)`：设置订阅类型为独占。独占订阅只允许一个消费者从主题中读取消息。
//        8. `build()`：构建PulsarSource对象并返回。
        PulsarSource<String> source = PulsarSource.builder()
                .setServiceUrl("pulsar://hadoop102:6650,hadoop103:6650,hadoop104:6650")
                .setAdminUrl("http://hadoop102:8080,hadoop103:8080,hadoop104:8080")
                .setStartCursor(StartCursor.earliest())
//                persistent://cdcs/common/eventpush-topic
                .setTopics("cdcs/common/eventpush-topic")
                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                .setSubscriptionName("my-subscription")
                .setSubscriptionType(SubscriptionType.Exclusive)
                .build();


        DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar Source");
        streamSource.print("PulsarSource>>>");
                env.execute();
    }
}
