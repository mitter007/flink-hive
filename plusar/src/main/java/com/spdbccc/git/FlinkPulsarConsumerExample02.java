package com.spdbccc.git;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class FlinkPulsarConsumerExample02 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Pulsar服务的URL
        String pulsarUrl = "pulsar://hadoop102:6650,hadoop103:6650,hadoop104:6650";

        // Pulsar主题的名称
        String topicName = "persistent://cdcs/common/eventpush-topic";

        // Pulsar集群的认证Token
//        String token = "your-token";

        // 创建Pulsar客户端
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
//                .authentication(AuthenticationFactory.token(token))
                .build();

        // 创建消费者
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName("flink-subscription")
                .subscribe();

        // 创建Pulsar数据流
        DataStream<byte[]> pulsarStream = env.addSource(new PulsarSourceFunction(consumer));

        // 对Pulsar数据流进行处理
        pulsarStream.map(new MapFunction<byte[], String>() {
            @Override
            public String map(byte[] value) throws Exception {
                // 处理接收到的消息
                return new String(value);
            }
        }).print();

        // 执行Flink程序
        env.execute("Flink Pulsar ");
    }

    // 自定义PulsarSourceFunction
    public static class PulsarSourceFunction extends RichSourceFunction<byte[]> {
        private Boolean running = true;

        private final Consumer<byte[]> consumer;

        public PulsarSourceFunction(Consumer<byte[]> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run(SourceContext<byte[]> ctx) throws Exception {
            while (running) {
                Message<byte[]> message = consumer.receive();
                byte[] value = message.getData();
                ctx.collect(value);
                consumer.acknowledge(message);
            }
        }

        @Override
        public void cancel() {

            running = false;
            try {
                consumer.close();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }

        }
    }
}
