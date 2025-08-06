package com.spdbccc;

import org.apache.pulsar.client.api.*;

/**
 * @author tuzuoquan
 * @version 1.0
 * @ClassName PulsarConsumerTest
 * @description 演示Pulsar的消费者的使用  (在使用之前，先使用上产者生产数据)
 * @date 2022/8/23 23:33
 **/
public class PulsarConsumerTest {

    public static void main(String[] args) throws PulsarClientException {
        //1.创建pulsar的客户端的对象
        PulsarClient pulsarClient =
                PulsarClient.builder().serviceUrl("pulsar://hadoop102:6650,hadoop102:6650,hadoop102:6650").build();

        //2.基于客户端构建消费者对象
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://cdcs/common/eventpush-topic")
                //下面的名称可以随便写
                .subscriptionName("sub_01")
                .subscribe();


        //3.循环从消费者读取数据
        while(true) {
            //3.1:接收消息
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
