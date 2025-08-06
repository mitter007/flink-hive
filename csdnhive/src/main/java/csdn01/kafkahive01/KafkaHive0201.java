package csdn01.kafkahive01;

import com.alibaba.fastjson.JSONObject;
import csdn01.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

import java.time.Duration;
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

//+----+--------------------------------+----------------------+-------------+-------------------------+
//| op |                             id |                   ts |          vc |                      pt |
//+----+--------------------------------+----------------------+-------------+-------------------------+
//| +I |                             27 |            100000000 |          18 | 2023-08-15 16:30:23.356 |
//| +I |                             27 |            100000000 |          18 | 2023-08-15 16:30:23.356 |
//| +I |                             27 |            100000000 |          18 | 2023-08-15 16:30:23.356 |
//| +I |                             27 |            100000000 |          18 | 2023-08-15 16:30:23.356 |
//| +I |                             27 |            100000000 |          18 | 2023-08-15 16:30:33.517 |
//| +I |                             27 |            100000000 |          18 | 2023-08-15 16:30:36.193 |
//| +I |                             27 |            100000000 |          18 | 2023-08-15 16:30:38.128 |
//| +I |                             27 |            100000000 |          18 | 2023-08-15 16:30:40.343 |
public class KafkaHive0201 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS t6hiveTable ("

                + "id STRING,"

                + "ts bigint,"
                + "vc int"
                + ")"

                + " partitioned by (dt string) stored as parquet tblproperties ("

                + " 'partition.time-extractor.timestamp-pattern'='$dt'," // hive 分区提取器提取时间戳的格式

                + " 'sink.partition-commit.trigger'='process-time'," // 分区触发提交的类型可以指定 "process-time" 和 "partition-time" 处理时间和分区时间

                + " 'sink.partition-commit.delay'='5s'," // 提交延迟
//                + " 'table.exec.source.idle-timeout'='10s'," //  如果 topic 中的某些分区闲置，watermark 生成器将不会向前推进。 你可以在表配置中设置 'table.exec.source.idle-timeout' 选项来避免上述问题
                + " 'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', " //-- Assume user configured time zone is 'Asia/Shanghai'
                + " 'sink.partition-commit.policy.kind'='metastore,success-file'" // 提交类型

                + ")");

//指定方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);


    /*    DataStreamSource<WaterSensor> streamSource = env.fromElements(new WaterSensor("1001", 12L, 2), new WaterSensor("1002", 13L, 3), new WaterSensor("1003", 14L,
                3));
        Table table = tableEnv.fromDataStream(streamSource, "id,ts,vc");
        tableEnv.createTemporaryView("kafka01",table);

        //同步两张表
        tableEnv.executeSql("INSERT INTO t4hiveTable "
                + "SELECT id,ts,vc FROM kafka01").print();*/
        //创建kafka的表
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.reset","latest");
        //kerberos认证
        properties.setProperty("security.protocol","SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism","GSSAPI");
        properties.setProperty("sasl.kerberos.service.name","kafka");

        DataStream<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<>("csdn02", new SimpleStringSchema(),
                properties));
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream =
                kafkaStream.map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        String id = jsonObject.getString("id");
                        Long ts = jsonObject.getLong("ts");
                        Integer vc = jsonObject.getInteger("vc");
                        return new WaterSensor(id, ts, vc);
                    }
                });



        // 定义Watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((WaterSensor, timestamp) -> (long) WaterSensor.getTs());

        // 转换为带有时间戳的DataStream
        DataStream<WaterSensor> timestampedStream = waterSensorDStream
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 将DataStream转换为表，并添加处理时间时间戳字段
        tableEnv.createTemporaryView("myTable", timestampedStream, "id, ts, vc, pt.proctime");
//        tableEnv.executeSql("select * from myTable").print();

//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        TableResult tableResult = tableEnv.executeSql("INSERT INTO t6hiveTable "
                + "SELECT id,ts,vc,DATE_FORMAT(pt, 'yyyy-MM-dd') FROM myTable");


//        tableResult.getJobClient().get().getJobExecutionResult().get();

        // 将DataStream转换为表，并添加处理时间时间戳字段
//        tableEnv.createTemporaryView("kafka01", watermarks, "id, ts, vc, pt.proctime");


//        tableEnv.executeSql( "SELECT id,ts,vc,DATE_FORMAT(pt, 'yyyy-MM-dd') FROM kafka01").print();





//        tableEnv.insertInto(tablename,);



    }
}
