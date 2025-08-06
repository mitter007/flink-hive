package com.spdbccc;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.Properties;

/**
 * ClassName: KafkaHive01
 * Package: csdn01.kafkahive01
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/14 16:18
 * @Version 1.0
 */

//hive 里面没有出现kafka01这张表
public class KafkaHive03 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();

        // 第一种方式创建----------start------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//        使用StreamExecutionEnvironment创建StreamTableEnvironment，必须设置StreamExecutionEnvironment的checkpoint
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        Configuration configuration = tableEnv.getConfig().getConfiguration();

        configuration.setString("table.exec.hive.fallback-mapred-reader", "true");
        //如果 topic 中的某些分区闲置，watermark 生成器将不会向前推进。 你可以在表配置中设置 'table.exec.source.idle-timeout' 选项来避免上述问题

        configuration.setString("table.exec.source.idle-timeout", "10s");


        // 1.创建HiveCatalog
        String name = "myhive";
        String defaultDatabase = "catalog";
        String hiveConfDir = "D:\\code\\workspace_exer\\flink-hive\\csdnhive\\src\\main\\resources\\";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        // 2.注册HiveCatalog
        tableEnv.registerCatalog(name, hive);
        // 3.把HiveCatalog: myhive作为当前session的catalog
        tableEnv.useCatalog(name);
        tableEnv.useDatabase(defaultDatabase);
        //指定方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
// 5. 建表sql以hive为目的地

        tableEnv.executeSql("drop table if exists t7hiveTable");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS t7hiveTable ("

                + "id STRING,"

                + "ts bigint,"
                + "vc int"
                + ")"

                + " STORED AS parquet "

        );

//指定方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.reset","latest");
        //kerberos认证
 /*       properties.setProperty("security.protocol","SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism","GSSAPI");
        properties.setProperty("sasl.kerberos.service.name","kafka");*/

//        DataStreamSource<String> streamSource = env.socketTextStream("hadoop103", 7777);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setGroupId("221109")
                .setTopics("csdn02")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");


        SingleOutputStreamOperator<WaterSensor> mapSource = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String id = jsonObject.getString("id");
                Long ts = jsonObject.getLong("ts");
                Integer vc = jsonObject.getInteger("vc");
                return new WaterSensor(id, ts, vc);
            }
        });

        Table table = tableEnv.fromDataStream(mapSource, "id,ts,vc");
        tableEnv.createTemporaryView("kafka01",table);

        //同步两张表
        tableEnv.executeSql("INSERT INTO t7hiveTable "
                + "SELECT id,ts,vc FROM kafka01").print();



    }
}
