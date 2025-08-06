package com.spdbccc.test02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @project_name: flinkDemo
 * @package_name: com.wmy.flink.sql.flinksql.catalog.hive
 * @Author: wmy
 * @Date: 2021/10/10
 * @Major: 数据科学与大数据技术
 * @Post：大数据实时开发
 * @Email：wmy_2000@163.com
 * @Desription: flink 连接hive第一个案例
 * @Version: wmy-version-03
 */
public class Hive_Catalog_03 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        tableEnv.getConfig().setString("table.exec.source.idle-timeout", "10s");


        tableEnv.executeSql("drop table if exists KafkaTable");
        tableEnv.executeSql("" +
                "CREATE TABLE KafkaTable (\n" +
                "  `id` BIGINT,\n" +
                "  `name` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'test01',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");
        //2.创建HiveCatalog
//        tableEnv.sqlQuery("select * from KafkaTable").execute().print();
                String name = "myHive01";
                String defaultDatabase = "catalog";
                String hiveConfDir = "D:\\code\\workspace_exer\\flink-hive\\hive-catalog\\src\\main\\resources\\";
        HiveCatalog hiveCatalog = new HiveCatalog(name,defaultDatabase,hiveConfDir);
        //打包到集群测试
        //    HiveCatalog hiveCatalog = new HiveCatalog("myHive", "catalog", "/opt/module/hive/conf");

                //3.注册HiveCatalog
                tableEnv.registerCatalog("myHive01", hiveCatalog);
                //4.使用HiveCatalog
                tableEnv.useCatalog("myHive01");
                tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        //    建表 创建hive表
        //tableEnv.executeSql("CREATE TABLE IF NOT EXISTS t_kafkaMsg2hiveTable (ip STRING,msg STRING)");
                tableEnv.executeSql("drop table if exists hivetable");
                tableEnv.executeSql("" +
                        "CREATE TABLE hivetable (\n" +
                        "  id BIGINT,\n" +
                        "  name STRING\n" +
                        ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (\n" +
                        "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
                        "  'sink.partition-commit.trigger'='partition-time',\n" +
                        "  'sink.partition-commit.delay'='1 h',\n" +
                        "  'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
                        ")");
        //创建kafka表
        //指定方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("drop table if exists KafkaTable");
        tableEnv.executeSql("" +
                "CREATE TABLE KafkaTable (\n" +
                "  `id` BIGINT,\n" +
                "  `name` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'test01',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");


//        tableEnv.sqlQuery("select * from t1").execute().print();

        tableEnv.executeSql("" +
                "insert into   hivetable select id,name,'a','b' from  KafkaTable");
        tableEnv.sqlQuery("select * from hivetable");


//    CREATE TABLE IF NOT EXISTS t_kafkaMsg2hiveTable (ip STRING,msg STRING)
//5.执行查询,查询Hive中已经存在的表数据
//        tableEnv.executeSql("select * from employee").print();


    }
}
