package com.spdbccc.dl;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.Random;

public class test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        env.setStateBackend(new FsStateBackend("file:///Users/xingxuanming/Downloads/flink-checkpoint/checkpoint"));

        Properties props_source = new Properties();
        props_source.setProperty("bootstrap.servers", "node1:9092");
        props_source.setProperty("group.id", "flink");
        props_source.setProperty("auto.offset.reset", "latest");
        //会开启一个后台线程每隔5s检测一下Kafka的分区情况
        props_source.setProperty("flink.partition-discovery.interval-millis", "5000");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("flink_kafka", new SimpleStringSchema(), props_source);
        kafkaSource.setStartFromLatest();
        //2.输入端保证：执行Checkpoint的时候提交offset到Checkpoint(Flink用)
        kafkaSource.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //value就是每一行
                String[] words = value.split(" ");
                for (String word : words) {
                    Random random = new Random();
                    int i = random.nextInt(5);
                    if (i > 3) {
                        System.out.println("出bug了...");
                        throw new RuntimeException("出bug了...");
                    }
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> groupedDS = wordAndOneDS.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> aggResult = groupedDS.sum(1);
        SingleOutputStreamOperator<String> result = (SingleOutputStreamOperator<String>) aggResult.map(new RichMapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                return value.f0 + ":::" + value.f1;
            }
        });

        //3.输出端保证
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
        result.addSink(kafkaSink);

        env.execute();
    }
}

