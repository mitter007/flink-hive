package com.spdbccc;

import org.apache.pulsar.client.api.*;

public class PulsarConsumer {

    public static void main(String[] args) throws PulsarClientException {

        // Pulsar服务的URL
        String pulsarUrl = "pulsar://hadoop102:6650,hadoop102:6650,hadoop102:6650";

        // Pulsar主题的名称
        String topicName = "persistent://public/default/test-token";
        String token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.mBIlkB_VhO1TrOQ0PhPOvNlqN5Ko_1_E4VFmdObwdyMJKVNyt2yZjWxJvrLDLHxtTusisFuTUTorVFBQkVvHHhQpzqVUKhORbs5TDswrB2OS3casooUNFbB4R868e6cEaH3yglUiIKRZEDbthqx-f_7zOnf9wYZSMKrHpSld1xKoo6-1G1yNdJ7J_wH5k9ouVaWSMu1z956ELq_vRQycpL4_I51GRpz5vhMtufzQIeRq83Ewd_cyDKO8SHV7SAfAjMuXbj8CVwU-CrH7BzIhMcUUmDszIWBPB6_qFPzokmnWBdokWW7GNE9qLQKGd8Dk-Z3EMqSF332M0hOLDsU1Dw";

        // 创建Pulsar客户端
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .authentication(AuthenticationFactory.token(token))
                .build();

        // 创建消费者
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName("my-subscription")
                .subscribe();

        // 持续消费消息
        while (true) {
            Message<byte[]> message = consumer.receive();

            try {
                // 处理接收到的消息
                byte[] data = message.getData();
                System.out.println("Received message: " + new String(data));
                consumer.acknowledge(message);
            } catch (Exception e) {
                // 处理消息时发生异常
                consumer.negativeAcknowledge(message);
            }
        }

        // 关闭消费者和Pulsar客户端
    }
}
