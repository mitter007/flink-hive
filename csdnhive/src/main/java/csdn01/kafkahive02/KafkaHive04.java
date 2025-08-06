package csdn01.kafkahive02;

import com.alibaba.fastjson.JSONObject;
import csdn01.bean.WaterSensor;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import javax.security.auth.kerberos.KerberosKey;
import javax.security.auth.kerberos.KerberosPrincipal;
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

//就是写不进去
public class KafkaHive04 {
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

        tableEnv.executeSql("drop table if exists t6hiveTable");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS t6hiveTable ("

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
        properties.setProperty("group.id", "109");
        properties.setProperty("auto.offset.reset","latest");

        //kerberos认证
 /*       properties.setProperty("security.protocol","SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism","GSSAPI");
        properties.setProperty("sasl.kerberos.service.name","kafka");*/
        Properties kakfaPro = new Properties();
        kakfaPro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102");
        kakfaPro.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"test");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
//        properties.setProperty(K)

        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<>("csdn02",
                new SimpleStringSchema(), kakfaPro));




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
        tableEnv.executeSql("INSERT INTO t6hiveTable "
                + "SELECT id,ts,vc FROM kafka01").print();



    }
}
