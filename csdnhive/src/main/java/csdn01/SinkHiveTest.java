package csdn01;
 
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
 
import java.time.Duration;
 
public class SinkHiveTest {
    public static void main(String[] args) {
 
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
 
        // 第一种方式创建----------start------------------
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        String jarFile = "D:\\ljpPro\\frauddetection-0.1.jar";
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("192.168.37.103", 8081, jarFile);
//        使用StreamExecutionEnvironment创建StreamTableEnvironment，必须设置StreamExecutionEnvironment的checkpoint
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        Configuration configuration = tableEnv.getConfig().getConfiguration();

        configuration.setString("table.exec.hive.fallback-mapred-reader", "true");
        //如果 topic 中的某些分区闲置，watermark 生成器将不会向前推进。 你可以在表配置中设置 'table.exec.source.idle-timeout' 选项来避免上述问题

        configuration.setString("table.exec.source.idle-timeout", "10s");
        // 第一种方式创建------------------end----------------------
 
        // 第二种方式创建----------start------------------
//        TableEnvironment tableEnv = TableEnvironment.create(settings);
//        Configuration configuration = tableEnv.getConfig().getConfiguration();
//        configuration.setString("table.exec.hive.fallback-mapred-reader", "true");
//        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
//        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(2));
        // 第二种方式创建------------------end----------------------
 
 
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
 
        tableEnv.executeSql("drop table if exists t_kafkaMsg2hiveTable");
 
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS t_kafkaMsg2hiveTable ("
 
                + "ip STRING,"
 
                + "msg STRING"
 
                + ")"
 
                + " PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES ("
 
                + " 'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00'," // hive 分区提取器提取时间戳的格式
 
                + " 'sink.partition-commit.trigger'='partition-time'," // 分区触发提交的类型可以指定 "process-time" 和 "partition-time" 处理时间和分区时间
 
                + " 'sink.partition-commit.delay'='5s'," // 提交延迟
//                + " 'table.exec.source.idle-timeout'='10s'," //  如果 topic 中的某些分区闲置，watermark 生成器将不会向前推进。 你可以在表配置中设置 'table.exec.source.idle-timeout' 选项来避免上述问题
                + " 'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', " //-- Assume user configured time zone is 'Asia/Shanghai'
                + " 'sink.partition-commit.policy.kind'='metastore,success-file'" // 提交类型
 
                + ")");
 
 
//指定方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
 
 
        // 4.建表sql以kafka为数据源，创建后就会监听kafka并写入数据至flink的内存
 
        tableEnv.executeSql("drop table if exists t_KafkaMsgSourceTable");
 
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS t_KafkaMsgSourceTable ("
                + "ip STRING"
                + ",msg STRING"
                + ",ts BIGINT" //13位原始时间戳
//                + ",ts3 AS TO_TIMESTAMP(FROM_UNIXTIME(ts/ 1000, 'yyyy-MM-dd HH:mm:ss'))" //flink的TIMESTAMP(3)格式
                + ",ts3 AS TO_TIMESTAMP_LTZ(ts, 3)" //flink的TIMESTAMP(3)格式
                + ",WATERMARK FOR ts3 AS ts3 - INTERVAL '5' SECOND" //水印最迟5s
                + ")"
                + " WITH ("
                + " 'connector' = 'kafka',"
                + " 'topic' = 'test',"
                + " 'properties.bootstrap.servers' = 'hadoop102:9092',"
                + " 'properties.group.id' = 'kafkaflinkhivedemo',"
                + " 'scan.startup.mode' = 'earliest-offset'," //earliest-offset
                + " 'format' = 'json',"
                + " 'json.ignore-parse-errors' = 'true'"
                + ")");
 
 
        // 6. 同步2个表
        tableEnv.executeSql("INSERT INTO t_kafkaMsg2hiveTable "
                + "SELECT ip,msg,DATE_FORMAT(ts3, 'yyyy-MM-dd'), DATE_FORMAT(ts3, 'HH') FROM t_KafkaMsgSourceTable").print();
 
//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        tableEnv.executeSql("alter table t_kafkamsg2hivetable add partition(dt='2022-03-04',hr=11)");
//        tableEnv.executeSql("SELECT * FROM t_kafkamsg2hivetable WHERE dt='2022-03-04' and hr='11'").print();
//        tableEnv.executeSql("select * from t_KafkaMsgSourceTable").print();
 
    }
}