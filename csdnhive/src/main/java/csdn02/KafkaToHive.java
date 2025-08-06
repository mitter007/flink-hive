package csdn02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.Duration;

public class KafkaToHive {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.12以后默认都是EventTime,这是过期方法,并且不在提IngestionTime
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //如果要使用ProcessingTime，可以关闭watermark
        env.getConfig().setAutoWatermarkInterval(0);

        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                                                    .useBlinkPlanner()
                                                    .inStreamingMode()
                                                    .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        //设置为exactly-once
        tEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20));
        Configuration configuration = tEnv.getConfig().getConfiguration();

        configuration.setString("table.exec.hive.fallback-mapred-reader", "true");

        //配置hive
        String catalogName = "myHive";
        String db = "catalog";
        String hiveConfPath = "D:\\code\\workspace_exer\\flink-hive\\hive-catalog\\src\\main\\resources\\";
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, db, hiveConfPath);
        //注册并使用
        tEnv.registerCatalog(catalogName,hiveCatalog);
        tEnv.useCatalog(catalogName);

//        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS stream");
        tEnv.executeSql("DROP TABLE IF EXISTS kafka_log");
        //创建kafka的表
        tEnv.executeSql("create table kafka_log(\n" +
                "user_id String,\n" +
                "order_amount Double,\n" +
                "order_total_amount Double,\n" +
                "log_ts Timestamp(3),\n" +
                "WATERMARK FOR log_ts AS log_ts -INTERVAL '5' SECOND\n" +
                " )WITH(\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'csdn02',\n" +
                " 'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                " 'properties.group.id' = 'flink1',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'format' = 'json'\n" +
//                " 'json.fail-on-missing-field' = 'false\n" +
//                " 'json.ignore-parse-errors' = 'true'\n" +
                " )");
        //开始在hive中创建表
          tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
          tEnv.executeSql("CREATE DATABASE IF NOT EXISTS catalog");
          tEnv.executeSql("DROP TABLE IF EXISTS catalog.hive_log");
          tEnv.executeSql(" create table catalog.hive_log(\n" +
                  " user_id String,\n" +
                  " order_amount double\n" +
                  " ) "
                          + " PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES ("

                          + " 'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00'," // hive 分区提取器提取时间戳的格式

                          + " 'sink.partition-commit.trigger'='partition-time'," // 分区触发提交的类型可以指定 "process-time" 和 "partition-time" 处理时间和分区时间

                          + " 'sink.partition-commit.delay'='1min'," // 提交延迟
//                + " 'table.exec.source.idle-timeout'='10s'," //  如果 topic 中的某些分区闲置，watermark 生成器将不会向前推进。 你可以在表配置中设置 'table.exec.source.idle-timeout' 选项来避免上述问题
                          + " 'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', " //-- Assume user configured time zone is 'Asia/Shanghai'
                          + " 'sink.partition-commit.policy.kind'='metastore,success-file'" // 提交类型

                          + ")");
          //将kafka中的数据插入到hive中
          tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
          tEnv.executeSql("insert into catalog.hive_log \n" +
                  "  select \n" +
                  "  user_id,\n" +
                  "  order_amount,\n" +
                  "  DATE_FORMAT(log_ts,'yyyy-MM-dd'),\n" +
                  "  DATE_FORMAT(log_ts,'HH')\n" +
                  "  from kafka_log");
    }
}
