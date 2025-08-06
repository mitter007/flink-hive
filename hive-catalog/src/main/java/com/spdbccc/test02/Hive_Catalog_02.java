package com.spdbccc.test02;

import com.alibaba.fastjson.JSONObject;
import com.spdbccc.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.Properties;

/**
 *
 */
public class Hive_Catalog_02 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


/*        tableEnv.executeSql("drop table if exists t1");
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


        tableEnv.sqlQuery("select * from t1").execute().print();*/
        //创建kafka表
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.reset", "latest");
        //kerberos认证
 /*       properties.setProperty("security.protocol","SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism","GSSAPI");
        properties.setProperty("sasl.kerberos.service.name","kafka");*/

        DataStream<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<>("csdn02", new SimpleStringSchema(),
                properties));
        kafkaStream.print();
        SingleOutputStreamOperator<WaterSensor> waterSensor = kafkaStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String id = jsonObject.getString("id");
                Long ts = jsonObject.getLong("ts");
                Integer vc = jsonObject.getInteger("vc");
                return new WaterSensor(id, ts, vc);
            }
        });

//        waterSensor.print("waterSensor>>>");
/*        Table table = tableEnv.fromDataStream(waterSensor, "id,ts,vc");
        tableEnv.createTemporaryView("t1", table);
        tableEnv.executeSql("select * from t1").print();*/

        HiveCatalog hiveCatalog = new HiveCatalog("myHive", "catalog01", "D:\\code\\workspace_exer\\flink-hive\\hive" +
                "-catalog\\src\\main\\resources");
        //打包到集群测试
        //    HiveCatalog hiveCatalog = new HiveCatalog("myHive", "catalog", "/opt/module/hive/conf");
        //3.注册HiveCatalog
        tableEnv.registerCatalog("myHive", hiveCatalog);
        //4.使用HiveCatalog
        tableEnv.useCatalog("myHive");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        //    建表 创建hive表
        //tableEnv.executeSql("CREATE TABLE IF NOT EXISTS t_kafkaMsg2hiveTable (ip STRING,msg STRING)");
        tableEnv.executeSql("drop table if exists hive_table");
        //第一种
/*        tableEnv.executeSql("" +
                "create table HiveSink(\n" +
                "id string,ts bigint,vc int\n" +
                ")\n" +
                "with (\n" +
                "'connector' = 'filesystem',\n" +
                "'path' = 'hdfs://hadoop102:8020/user/hive/test/HiveSink')");*/
        //第二种
        tableEnv.executeSql("" +
                "CREATE TABLE hive_table (\n" +
                "  id STRING,\n" +
                "  ts BIGINT,\n" +
                "  vc BIGINT\n" +
                ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (\n" +
                "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
                "  'sink.partition-commit.trigger'='partition-time',\n" +
                "  'sink.partition-commit.delay'='1 h',\n" +
                "  'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
                ")");



        //创建kafka表
        //指定方言
        Table table = tableEnv.fromDataStream(waterSensor, "id,ts,vc");
        tableEnv.createTemporaryView("t1", table);
        tableEnv.executeSql("select * from t1").print();
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);


//        tableEnv.sqlQuery("select * from t1").execute().print();

        tableEnv.executeSql("" +
                "insert into hive_table select id,ts,vc from  t1");


//    CREATE TABLE IF NOT EXISTS t_kafkaMsg2hiveTable (ip STRING,msg STRING)
//5.执行查询,查询Hive中已经存在的表数据
//        tableEnv.executeSql("select * from employee").print();


    }
}
