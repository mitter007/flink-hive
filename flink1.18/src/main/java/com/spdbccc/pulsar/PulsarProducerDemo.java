package com.spdbccc.pulsar;

import org.apache.pulsar.client.api.*;

public class PulsarProducerDemo {
    public static void main(String[] args) throws PulsarClientException {
        // 1. Pulsar 服务地址
        String serviceUrl = "pulsar://hadoop102:6650";
        String topic = "persistent://public/default/event";

        // 2. 创建 Pulsar 客户端
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        // 3. 创建生产者
        Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .create();

        System.out.println("🚀 已连接 Pulsar，开始发送消息...");

        // 4. 发送多条消息
        for (int i = 1; i <= 10; i++) {
            String content = "{\"id\":4,\"activity_name\":\"限时秒杀\",\"activity_type\":\"promotion\",\"activity_desc\":\"全场五折限时抢购\",\"event_time\":\"1766387454000\"}" ;
            MessageId msgId = producer.send(content.getBytes());
            System.out.printf("✅ 发送消息成功: [msgId=%s] 内容=%s\n", msgId, content);
        }

        // 5. 关闭资源
        producer.close();
        client.close();

        System.out.println("🎉 所有消息发送完毕，连接关闭。");
    }
}
