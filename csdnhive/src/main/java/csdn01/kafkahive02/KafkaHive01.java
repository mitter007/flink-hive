package csdn01.kafkahive02;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;

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
public class KafkaHive01 {
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

        tableEnv.executeSql("drop table if exists t1hiveTable");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS t1hiveTable ("

                + "id STRING,"

                + "ts STRING,"
                + "vc STRING"
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

//        DataStreamSource<WaterSensor> streamSource = env.fromElements(new WaterSensor("1001", 12L, 2), new WaterSensor("1002", 13L, 3), new WaterSensor("1003", 14L,
//                3));
//        Table table = tableEnv.fromDataStream(streamSource, "id,ts,vc");
//        tableEnv.createTemporaryView("kafka01",table);

//        //同步两张表
//        tableEnv.executeSql("" +
//                "INSERT INTO t1hiveTable(id,ts,vc) values('13','1','1')").print();

//        tableEnv.executeSql("" + "INSERT INTO t1hiveTable(id,ts,vc) values('zz','c','v')").print();
        tableEnv.executeSql("" + "insert into activity_info(activity_name,start_time,create_time,activity_type," +
                "activity_desc,end_time,id) values ('中秋','','','3104','满减','','6')");
        tableEnv.executeSql("" + "insert into activity_info(activity_name,start_time,create_time,activity_type," +
                "activity_desc,end_time,id) values ('中秋','','','3104','满减','','7')");
        tableEnv.executeSql("" + "insert into activity_info(activity_name,start_time,create_time,activity_type," +
                "activity_desc,end_time,id) values ('中秋','','','3104','满减','','8')");
        tableEnv.executeSql("" + "insert into activity_info(activity_name,start_time,create_time,activity_type," +
                "activity_desc,end_time,id) values ('中秋','','','3104','满减','','10')");
        tableEnv.executeSql("select * from activity_info").print();





    }
}
