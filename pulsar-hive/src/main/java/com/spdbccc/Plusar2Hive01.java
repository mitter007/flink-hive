package com.spdbccc;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
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
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * ClassName: plusar
 * Package: com.spdbccc
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/8 18:48
 * @Version 1.0
 */
public class Plusar2Hive01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        // 第一种方式创建----------start-----------------


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
//        String hiveConfDir = "D:\\code\\workspace_exer\\flink-hive\\csdnhive\\src\\main\\resources\\";
        String hiveConfDir = "/opt/module/hive/conf";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        // 2.注册HiveCatalog
        tableEnv.registerCatalog(name, hive);
        // 3.把HiveCatalog: myhive作为当前session的catalog
        tableEnv.useCatalog(name);
        tableEnv.useDatabase(defaultDatabase);
        //指定方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
// 5. 建表sql以hive为目的地

        tableEnv.executeSql("drop table if exists hive_pulsar01");

        tableEnv.executeSql("" +
                "CREATE EXTERNAL TABLE hive_pulsar01\n" +
                "(\n" +
                "  customerId   string, \n" +
                "  cardNumber   string,\n" +
                "  accountNumber    string, \n" +
                "  scenarioCode string, \n" +
                "  eventCode    string, \n" +
                "  eventTime    string, \n" +
                "  activities   string, \n" +
                "  activityCode string,\n" +
                "  childActivities  string, \n" +
                "  childActivityCode    string, \n" +
                "  activityRefuseCode   string, \n" +
                "  lables   string,\n" +
                "  lableCode    string, \n" +
                "  ruleExpStr   string,\n" +
                "  LableResult  string,\t\n" +
                "  errCode  string,\t\n" +
                "  errMsg   string\n" +
                ") COMMENT 'pulsar01表'\n" +
                "    STORED AS parquet"

        );



        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        PulsarSource<String> source = PulsarSource.builder()
                .setServiceUrl("pulsar://hadoop102:6650,hadoop103:6650,hadoop104:6650")
                .setAdminUrl("http://hadoop102:8080,hadoop103:8080,hadoop104:8080")
                .setStartCursor(StartCursor.latest())
                .setTopics("cdcs/common/eventpush-topic")
                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                .setSubscriptionName("my-subscription")
                .setSubscriptionType(SubscriptionType.Exclusive)
                .build();
        DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar Source");
//        streamSource.print();

        SingleOutputStreamOperator<PulsarEvent> mapSource = streamSource.map(new MapFunction<String, PulsarEvent>() {
            @Override
            public PulsarEvent map(String value) throws Exception {
//                JSONObject jsonObject = JSONObject.parseObject(value);
//                String customerId = jsonObject.getString("customerId");
//                String cardNumber = jsonObject.getString("cardNumber");
//                String accountNumber = jsonObject.getString("accountNumber");
//                String scenarioCode = jsonObject.getString("scenarioCode");
//                String eventCode = jsonObject.getString("eventCode");
//                String eventTime = jsonObject.getString("eventTime");
//                String activities = jsonObject.getString("activities");
//                String activityCode = jsonObject.getString("activityCode");
//                String childActivities = jsonObject.getString("childActivities");
//                String childActivityCode = jsonObject.getString("childActivityCode");
//                String activityRefuseCode = jsonObject.getString("activityRefuseCode");
//                String lables = jsonObject.getString("lables");
//                String lableCode = jsonObject.getString("lableCode");
//                String ruleExpStr = jsonObject.getString("ruleExpStr");
//                String lableResult = jsonObject.getString("LableResult");
//                String errCode = jsonObject.getString("errCode");
//                String errMsg = jsonObject.getString("errMsg");
//
//
//                return new PulsarEvent(customerId, cardNumber, accountNumber, scenarioCode, eventCode, eventTime,
//                        activities, activityCode, childActivities, childActivityCode, activityRefuseCode, lables, lableCode
//                        , ruleExpStr, lableResult, errCode, errMsg);

                String[] words = value.split(" ");
                return new PulsarEvent(words[0],words[1],words[2],words[3],words[4],words[5],words[6],words[7],
                        words[8],words[9],words[10],words[11],words[12],words[13],words[14],words[15],words[16]);


            }
        });


        Table table = tableEnv.fromDataStream(mapSource, "customerId,cardNumber,accountNumber,scenarioCode,eventCode,eventTime,activities,activityCode,childActivities,childActivityCode,activityRefuseCode,lables,lableCode,ruleExpStr,LableResult,errCode,errMsg");
        tableEnv.createTemporaryView("kafka01",table);

        //同步两张表
        tableEnv.executeSql("INSERT INTO hive_pulsar01 "
                + "SELECT customerId,cardNumber,accountNumber,scenarioCode,eventCode,eventTime,activities,activityCode,childActivities,childActivityCode,activityRefuseCode,lables,lableCode,ruleExpStr,LableResult,errCode,errMsg FROM kafka01").print();

//                env.execute();
    }
}
