package com.spdbccc.dl;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * ClassName: flink01
 * Package: com.spdbccc.dl
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/9/14 14:09
 * @Version 1.0
 */
public class flink01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        Flink内部
        //1.flink内部保证精准一次：设置检查点
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //10s内保存一次检查点
        checkpointConfig.setCheckpointInterval(10000);
        //保存检查点的超时时间为5秒钟
        checkpointConfig.setCheckpointTimeout(5000);
        //新一轮检查点开始前最少等待上一轮保存15秒才开始
        checkpointConfig.setMinPauseBetweenCheckpoints(15000);
        //设置作业失败后删除检查点(默认)
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //设置检查点模式为精准一次(默认)
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


        Properties props_source = new Properties();
        props_source.setProperty("bootstrap.servers", "node1:9092");
        props_source.setProperty("group.id", "flink");
        props_source.setProperty("auto.offset.reset", "latest");
        //会开启一个后台线程每隔5s检测一下Kafka的分区情况
        props_source.setProperty("flink.partition-discovery.interval-millis", "5000");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("flink_kafka", new SimpleStringSchema(),
                props_source);
        kafkaSource.setStartFromLatest();
        //2.输入端保证：执行Checkpoint的时候提交offset到Checkpoint(Flink用)
        kafkaSource.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);


        Properties props_sink = new Properties();
        props_sink.setProperty("bootstrap.servers", "node1:9092");
        //3.1 设置事务超时时间，也可在kafka配置中设置
        props_sink.setProperty("transaction.timeout.ms", 1000 * 5 + "");
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(
                "flink_kafka2",
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
                props_sink,
                //3.2 设置输出的的语义为精准一次
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        kafkaDS.addSink(kafkaSink);



    }
}
