package com.spdbccc.git.git01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.pulsar.client.api.*;

public class FlinkPulsarConsumerExample {

    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Pulsar服务的URL
        String pulsarUrl = "pulsar://hadoop102:6650";

        // Pulsar主题的名称
        String topicName = "persistent://public/default/test-token";

        // Pulsar集群的认证Token
        String token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.mBIlkB_VhO1TrOQ0PhPOvNlqN5Ko_1_E4VFmdObwdyMJKVNyt2yZjWxJvrLDLHxtTusisFuTUTorVFBQkVvHHhQpzqVUKhORbs5TDswrB2OS3casooUNFbB4R868e6cEaH3yglUiIKRZEDbthqx-f_7zOnf9wYZSMKrHpSld1xKoo6-1G1yNdJ7J_wH5k9ouVaWSMu1z956ELq_vRQycpL4_I51GRpz5vhMtufzQIeRq83Ewd_cyDKO8SHV7SAfAjMuXbj8CVwU-CrH7BzIhMcUUmDszIWBPB6_qFPzokmnWBdokWW7GNE9qLQKGd8Dk-Z3EMqSF332M0hOLDsU1Dw";

        // 创建Pulsar数据流
        DataStream<byte[]> pulsarStream = env.addSource(new PulsarSourceFunction(topicName, pulsarUrl, token));

        // 对Pulsar数据流进行处理
        pulsarStream.map(new MapFunction<byte[], String>() {
            @Override
            public String map(byte[] value) throws Exception {
                // 处理接收到的消息
                return new String(value);
            }
        }).print();

        // 执行Flink程序
        env.execute("Flink Pulsar Consumer Example");
    }

    // 自定义PulsarSourceFunction
    public static class PulsarSourceFunction extends RichSourceFunction<byte[]> {

        private final String topicName;
        private final String pulsarUrl;
        private final String token;
        private transient PulsarClient client;
        private transient Consumer<byte[]> consumer;

        public PulsarSourceFunction(String topicName, String pulsarUrl, String token) {
            this.topicName = topicName;
            this.pulsarUrl = pulsarUrl;
            this.token = token;
            this.client = null;
            this.consumer = null;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.client = PulsarClient.builder()
                    .serviceUrl(pulsarUrl)
                    .authentication(AuthenticationFactory.token(token))
                    .build();

            this.consumer = client.newConsumer()
                    .topic(topicName)
                    .subscriptionName("flink-subscription-" + getRuntimeContext().getIndexOfThisSubtask())
                    .subscribe();
        }

        @Override
        public void run(SourceContext<byte[]> ctx) throws Exception {
            while (true) {
                Message<byte[]> message = consumer.receive();
                byte[] value = message.getData();
                ctx.collect(value);
                consumer.acknowledge(message);
            }
        }

        @Override
        public void cancel() {
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }
            if (client != null) {
                try {
                    client.close();
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
