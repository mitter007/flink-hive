package com.spdbccc.kafka;

import com.alibaba.fastjson.JSON;
import com.spdbccc.common.Constant;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

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
public class KafkaProducer {
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
        //把发送的key从字符串序列化为字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //把发送消息value从字符串序列化为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);

        int msgNum = arg;
//        final CountDownLatch countDownLatch = new CountDownLatch(msgNum);
        for (int i = 11; i <= msgNum; i++) {
            KafkaBean bean = new KafkaBean(i, "ad", "jiaowt@163.com", 1000 + i,System.currentTimeMillis()/1000);
            //指定发送分区
            /*ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(TOPIC_NAME
                    , 0, order.getOrderId().toString(), JSON.toJSONString(order));*/
            //未指定发送分区，具体发送的分区计算公式：hash(key)%partitionNum
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("test01"
                    , JSON.toJSONString(bean));

            //等待消息发送成功的同步阻塞方法
            RecordMetadata metadata = null;
            try {
                metadata = producer.send(producerRecord).get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.out.println("同步方式发送消息结果：" + "topic-" + metadata.topic() + "|partition-"
                    + metadata.partition() + "|offset-" + metadata.offset());

            //异步回调方式发送消息 ,

/*
            producer.send(producerRecord, new Callback() {
                // 回调函数会在 producer 收到 ack 时调用，为异步调用，该方法有两个参数，分别是元数据信息（RecordMetadata）和异常信息（Exception），
                // 如果 Exception 为 null，说明消息发送成功，如果 Exception 不为 null，说明消息发送失败
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.err.println("发送消息失败：" + exception.getStackTrace());
                    }
                    if (metadata != null) {
                        System.out.println("异步方式发送消息结果：" + "topic-" + metadata.topic() + "|partition-"
                                + metadata.partition() + "|offset-" + metadata.offset());
                    }
                    countDownLatch.countDown();
                }
            });
*/

            //送积分 TODO


            Thread.sleep(1000);
        }
            producer.close();

    }

}
