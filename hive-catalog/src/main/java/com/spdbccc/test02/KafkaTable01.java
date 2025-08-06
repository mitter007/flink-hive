package com.spdbccc.test02;

import com.alibaba.fastjson.JSON;
import com.spdbccc.bean.WaterSensor;
import jdk.internal.org.objectweb.asm.ClassReader;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.Duration;

/**
 * ClassName: KafkaTable01
 * Package: com.spdbccc.test02
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/11 17:17
 * @Version 1.0
 */
public class KafkaTable01 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));


        tableEnv.executeSql("drop table if exists t1");
        TableResult kafkaTable = tableEnv.executeSql("" +
                "CREATE TABLE t1( id string, ts bigint , vc int )\n" +
                "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'test1' ,\n" +
                "  'scan.startup.mode' = 'earliest-offset' ,\n" +
                "'sink.partitioner' = 'fixed',\n" +
                "  'topic' = 'catalog01',\n" +
                "  'format' = 'json'\n" +
                ")");


////        kafkaTable.print();
//        TableResult KafkaTable = tableEnv.executeSql("" +
//                " CREATE TABLE KafkaTable (\n" +
//                "   `id` BIGINT,\n" +
//                "   `item_id` BIGINT,\n" +
//                "   `behavior` STRING\n" +
//                " ) WITH (\n" +
//                "   'connector' = 'kafka',\n" +
//                "   'topic' = 'catalog01',\n" +
//                "   'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
//                "   'properties.group.id' = 'testGroup',\n" +
//                "   'scan.startup.mode' = 'earliest-offset',\n" +
//                "   'format' = 'json'\n" +
//                " )");
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setGroupId("221109")
                .setTopics("sensor")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
 DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        SingleOutputStreamOperator<WaterSensor> mapstream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
//        Table table = tableEnv.fromDataStream(mapstream, "id,ts,vc");
        tableEnv.createTemporaryView("t1",mapstream);
        tableEnv.sqlQuery("select * from t1").execute().print();
//        env.execute();
        tableEnv.sqlQuery("select * from t1").execute().print();


    }
}
