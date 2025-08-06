package com.spdbccc;

import org.apache.pulsar.client.api.*;

public class PulsarConsumerExample {
    public static void main(String[] args) throws PulsarClientException {
        String pulsarUrl = "pulsar://hadoop102:6650,hadoop102:6650,hadoop102:6650";
        String topicName = "persistent://cdcs/common/eventpush-topic";
        String subscriptionName = "my-subscription";

      /*  PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarBrokerUrl)
                .build();

        ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subscriptionName);

        Consumer<String> consumer = consumerBuilder.subscribe();*/
        // Pulsar集群的认证Token
        String token = "your-token";

        // 创建Pulsar客户端
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .authentication(AuthenticationFactory.token(token))
                .build();

//        PulsarClient client = PulsarClient.builder()
//                .serviceUrl(pulsarUrl)
//                .build();

        ConsumerBuilder<String> consumerBuilder = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subscriptionName);

        Consumer<String> consumer = consumerBuilder.subscribe();


        while (true) {
//            String message = consumer.receive().getValue();
//            System.out.println("Received message: " + message);
//            consumer.acknowledge(message);
            Message<String> message = consumer.receive();

            //3.2:获取消息
            String msg = message.getValue();

            //3.3: 处理数据---业务操作
            System.out.println("消息数据为：" + msg);

            //3.4: ack确认操作
            consumer.acknowledge(message);

            //如果消费失败了, 可以采用try catch方式进行捕获异常, 捕获后, 可以进行告知没有消费
            //consumer.negativeAcknowledge(message);
        }
    }
}
