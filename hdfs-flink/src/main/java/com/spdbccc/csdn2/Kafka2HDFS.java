package com.spdbccc.csdn2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.ZoneId;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


//这个也写成功了         // 设置当前用户
////        System.setProperty("HADOOP_USER_NAME","root"); 把这句注释掉就可以了
public class Kafka2HDFS {
    public static void main(String[] args) throws Exception {
 
 
        // flink 的配置信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置检查点 检查点:就是回滚的时间设置 设置回滚的时间产生的小文件越多,生产中尽量设置大些  单位是毫秒
        env.enableCheckpointing(1000L);
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//
//// 设置精确一次模式
//        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//// 最小间隔时间500毫秒
//        checkpointConfig.setMinPauseBetweenCheckpoints(500);
//// 超时时间1分钟
//        checkpointConfig.setCheckpointTimeout(60000);
//// 同时只能有一个检查点
//        checkpointConfig.setMaxConcurrentCheckpoints(1);
//// 开启检查点的外部持久化保存，作业取消后依然保留
//        checkpointConfig.enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//// 启用不对齐的检查点保存方式
//        checkpointConfig.enableUnalignedCheckpoints();
//// 设置检查点存储，可以直接传入一个String，指定文件系统的路径
//        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/flink-checkpoint/checkpoint");

        // kafka的配置信息
        // Properties prop = new Properties();
        // prop.put("bootstrap.servers","192.168.88.151:9092");
        // prop.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // prop.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // prop.put("auto.offset.reset","latest");
        // prop.put("group.id","test-1");
        // String topic ="topic-test";
        // FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);
        // DataStreamSource<String> streamSource = env.addSource(consumer);
 
        // 设置当前用户
//        System.setProperty("HADOOP_USER_NAME","root");
 
        // 模拟数据
//        DataStreamSource<String> streamSource = env.socketTextStream("hadoop103", 7778);
//        DataStreamSource<String> streamSource = env.fromElements("1001 1 12", "1002 2 3", "1003 4 3", "1003 4 3", "1003 4 3", "1003 4 3", "1003 4 3", "1003 4 3", "1003 4 3", "1003 4 3", "1003 4 3", "1003 4 3", "1003 4 3", "1003 4 3", "1003 4 3");
        Properties properties = new Properties();
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setGroupId("221109")
                .setTopics("sensor")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();


        properties.setProperty("properties.security.protocol","SASL_PLAINTEXT");

        KafkaConsumer<Object, Object> objectObjectKafkaConsumer = new KafkaConsumer<>(properties);

        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        streamSource.print("source_");
        // 处理数据
        SingleOutputStreamOperator<String> streamOperator = streamSource.flatMap(new FlatMapFunction<String, CarInfo>() {
            @Override
            public void flatMap(String s, Collector<CarInfo> collector) throws Exception {
                String[] s1 = s.split(" ");
                collector.collect(new CarInfo(s1[0], Double.valueOf(s1[1]), Long.valueOf(s1[2])));
            }
        }).map(new MapFunction<CarInfo, String>() {
            @Override
            public String map(CarInfo carInfo) throws Exception {
                return carInfo.getCarNo()+","+carInfo.getSpeed()+","+carInfo.getTimestamp();
            }
        });
 
        
    // 设置回滚 文件大小 创建新文件条件
    DefaultRollingPolicy<String, String> defaultRollingPolicy = DefaultRollingPolicy.builder()
            .withInactivityInterval(TimeUnit.SECONDS.toMillis(10))  //10s空闲，就滚动写入新的文件
            .withRolloverInterval(TimeUnit.SECONDS.toMillis(5)) //不论是否空闲，超过30秒就写入新文件，默认60s。这里设置为30S
            .withMaxPartSize(1024) // 设置每个文件的最大大小 ,默认是128M。这里设置为1G
            .build();
 
    // 设置文件前后缀
   OutputFileConfig fileConfig = OutputFileConfig
           .builder()
           .withPartPrefix("data")
           .withPartSuffix(".txt")
           .build();
 
   // 配置hdfs 信息
//   final StreamingFileSink<String> sink = StreamingFileSink
//            .forRowFormat(new Path("hdfs://hadoop102:8020/user/hive/warehouse/basic.db/monitor_car_data/"),
//                    new SimpleStringEncoder<String>("UTF-8"))//设置文件路径，以及文件中的编码格式
//            .withBucketAssigner(new DateTimeBucketAssigner<>("yyyyMMdd", ZoneId.of("Asia/Shanghai")))//设置自定义分桶
//            .withRollingPolicy(defaultRollingPolicy)//设置文件滚动条件
//            .withOutputFileConfig(fileConfig)
//            .build();

        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("hdfs://hadoop102:8020/user/hive/warehouse/basic.db/monitor_car_data/"),
                        new SimpleStringEncoder<String>("UTF-8"))//设置文件路径，以及文件中的编码格式
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyyMMdd", ZoneId.of("Asia/Shanghai")))//设置自定义分桶
                .withRollingPolicy(defaultRollingPolicy)//设置文件滚动条件
                .withOutputFileConfig(fileConfig)
                .build();
 
 
        streamOperator.print("map_");
        streamOperator.addSink(sink).setParallelism(1);
        env.execute();
        Thread.sleep(10);
        
    }
}