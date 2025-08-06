package com.spdbccc.test02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
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
 * @Version: wmy-version-01
 */
public class Hive_Catalog_01 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //启用检查点
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setParallelism(1);
        Configuration configuration = tableEnv.getConfig().getConfiguration();

        configuration.setString("table.exec.hive.fallback-mapred-reader", "true");
        //如果 topic 中的某些分区闲置，watermark 生成器将不会向前推进。 你可以在表配置中设置 'table.exec.source.idle-timeout' 选项来避免上述问题

        configuration.setString("table.exec.source.idle-timeout", "10s");





        //2.创建HiveCatalog
//        tableEnv.sqlQuery("select * from t1").execute().print();

        HiveCatalog hiveCatalog = new HiveCatalog("myHive", "catalog", "D:\\code\\workspace_exer\\flink-hive\\hive-catalog\\src\\main\\resources");
        //打包到集群测试
        //    HiveCatalog hiveCatalog = new HiveCatalog("myHive", "catalog", "/opt/module/hive/conf");
                //3.注册HiveCatalog
                tableEnv.registerCatalog("myHive", hiveCatalog);
                //4.使用HiveCatalog
                tableEnv.useCatalog("myHive");
                tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        //    建表 创建hive表
        //tableEnv.executeSql("CREATE TABLE IF NOT EXISTS t_kafkaMsg2hiveTable (ip STRING,msg STRING)");
                tableEnv.executeSql("drop table if exists HiveSink");
                tableEnv.executeSql("" +
                        "create table HiveSink(id string,ts bigint,vc int) STORED AS parquet  TBLPROPERTIES ('table.exec.source.idle-timeout'='10s')");
        //创建kafka表
        //指定方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("drop table if exists t1");
        tableEnv.executeSql("" +
                "CREATE TABLE t1( id string, ts bigint , vc int )\n" +
                "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'test1' ,\n" +
                "  'scan.startup.mode' = 'latest-offset' ,\n" +
                "'sink.partitioner' = 'fixed',\n" +
                "  'topic' = 'catalog01',\n" +
                "  'format' = 'json'\n" +
                ")");


//        tableEnv.sqlQuery("select * from t1").execute().print();

        tableEnv.executeSql("" +
                "insert into HiveSink select id,ts,vc from  t1");


//    CREATE TABLE IF NOT EXISTS t_kafkaMsg2hiveTable (ip STRING,msg STRING)
//5.执行查询,查询Hive中已经存在的表数据
//        tableEnv.executeSql("select * from employee").print();


    }
}
