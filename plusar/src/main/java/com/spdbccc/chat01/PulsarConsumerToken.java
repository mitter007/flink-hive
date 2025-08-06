package com.spdbccc.chat01;

import org.apache.pulsar.client.api.*;

public class PulsarConsumerToken {

    public static void main(String[] args) throws PulsarClientException {

        // Pulsar服务的URL
        String pulsarUrl = "pulsar://hadoop102:6650,hadoop102:6650,hadoop102:6650";

        // Pulsar主题的名称
        String topicName = "persistent://cdcs/common/eventpush-topic";

        // Pulsar集群的认证Token
        String token = "your-token";

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
