package com.spdbccc.kafka;

import com.spdbccc.common.Constant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * ClassName: KafkaProducer01
 * Package: com.spdbccc.kafka
 * Description:
 *
 * @Author JWT
 * @Create 2025/8/7 11:40
 * @Version 1.0
 */
public class KafkaConsumer01 {
    public static void main(String[] args) throws InterruptedException {
        for (String arg : args) {

            System.out.println(arg);
        }
        System.out.println(args);
        getKafkaProducer(Integer.parseInt(args[0]));

    }

    private static void getKafkaProducer(Integer arg) throws InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);
         /*
         发出消息持久化机制参数
         */
        /*props.put(ProducerConfig.ACKS_CONFIG, "1");
         *//*
        发送失败会重试发送失败会重试，默认重试间隔100ms，重试能保证消息发送的可靠性，但是也可能造成消息重复发送，比如网络抖动，所以需要在 接收者那边做好消息接收的幂等性处理
      //注意：消息发送失败会自动重试，不需要我们在回调函数中手动重试
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        //重试间隔设置
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 300);
        //设置发送消息的本地缓冲区，
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        *//*
        kafka本地线程会从缓冲区取数据，批量发送到broker，
        设置批量发送消息的大小，默认值是16384，即16kb，就是说一个batch满了16kb就发送出去
        *//*
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        *//*
        默认值是0，意思就是消息必须立即被发送，但这样会影响性能
        一般设置10毫秒左右，就是说这个消息发送完后会进入本地的一个batch，如果10毫秒内，这个batch满了16kb就会随batch一起被发送出去
        如果10毫秒内，batch没满，那么也必须把消息发送出去，不能让消息的发送延迟时间太长
        *//*
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);*/


        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Arrays.asList("doit01"));

        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                record.value();
                record.timestamp();
                System.out.println(record);
            }
        }
        kafkaConsumer.close();

    }

}
