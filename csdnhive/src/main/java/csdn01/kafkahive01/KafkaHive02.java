package csdn01.kafkahive01;

import com.alibaba.fastjson.JSONObject;
import csdn01.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
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

//有处理时间不行，没有处理时间也不行
    //hive里面也没有出现kafka01这个表；

//从元素中读数据是可以写出到hive表里面的
public class KafkaHive02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //没有设置checkpoint
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        tableEnv.executeSql("drop table if exists t4hiveTable");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS t4hiveTable ("

                + "id STRING,"

                + "ts bigint,"
                + "vc int"
                + ")"

                + " STORED AS parquet "

        );

//指定方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);


        DataStreamSource<WaterSensor> streamSource = env.fromElements(new WaterSensor("1001", 12L, 2), new WaterSensor("1002", 13L, 3), new WaterSensor("1003", 14L,
                3));
        Table table = tableEnv.fromDataStream(streamSource, "id,ts,vc");
        tableEnv.createTemporaryView("kafka01",table);

        //同步两张表
        tableEnv.executeSql("INSERT INTO t4hiveTable "
                + "SELECT id,ts,vc FROM kafka01").print();



//        tableEnv.executeSql("select * from kafka01").print();
       /* WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((WaterSensor, timestamp) -> Long.parseLong(WaterSensor.getId()));
        SingleOutputStreamOperator<WaterSensor> watermarks = waterSensor.assignTimestampsAndWatermarks(watermarkStrategy);

        // 将DataStream转换为表，并添加处理时间时间戳字段
        tableEnv.createTemporaryView("kafka01", watermarks, "id, ts, vc, pt.proctime");*/


//        tableEnv.executeSql( "SELECT id,ts,vc,DATE_FORMAT(pt, 'yyyy-MM-dd') FROM kafka01").print();





//        tableEnv.insertInto(tablename,);



    }
}
