package com.spdbccc.pulsar;

import org.apache.pulsar.client.api.*;

public class PulsarConsumer {
    public static void main(String[] args) {
        String serviceUrl = "pulsar://hadoop102:6650";
        String topic = "test01";
        String subscriptionName = "my-subscription";

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build()) {

            Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topic)
                    .subscriptionName(subscriptionName)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscribe();

            System.out.println("消费者启动，开始监听消息...");

            while (true) {
                // 接收消息（默认等待时间：无限）
                Message<byte[]> msg = consumer.receive();

                try {
                    String content = new String(msg.getData());
                    System.out.printf("接收到消息: id=%s 内容=%s%n", msg.getMessageId(), content);

                    // 手动确认消息已处理
                    consumer.acknowledge(msg);
                } catch (Exception e) {
                    System.err.printf("消息处理失败: %s%n", e.getMessage());
                    consumer.negativeAcknowledge(msg);
                }
            }

        } catch (PulsarClientException e) {
            System.err.println("Pulsar 客户端出错: " + e.getMessage());
        }
    }
}
