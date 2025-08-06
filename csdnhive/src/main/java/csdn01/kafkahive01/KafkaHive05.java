package csdn01.kafkahive01;

import com.alibaba.fastjson.JSONObject;
import csdn01.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

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



//从流中读数据，创建表，看看表啥样的
// +----+--------------------------------+----------------------+-------------+
//| op |                             id |                   ts |          vc |
//+----+--------------------------------+----------------------+-------------+
//| +I |                             27 |              1000000 |          18 |
//| +I |                             27 |              1000000 |          18 |
    //表出来是这个样子
public class KafkaHive05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Configuration configuration = tableEnv.getConfig().getConfiguration();

        configuration.setString("table.exec.hive.fallback-mapred-reader", "true");
        //如果 topic 中的某些分区闲置，watermark 生成器将不会向前推进。 你可以在表配置中设置 'table.exec.source.idle-timeout' 选项来避免上述问题

        configuration.setString("table.exec.source.idle-timeout", "10s");


        //指定方言 从kafka中读数据
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.reset","latest");
        //kerberos认证
 /*       properties.setProperty("security.protocol","SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism","GSSAPI");
        properties.setProperty("sasl.kerberos.service.name","kafka");*/

        DataStream<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("csdn02",
                new SimpleStringSchema(),
                properties));

        SingleOutputStreamOperator<WaterSensor> mapSource = kafkaStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String id = jsonObject.getString("id");
                Long ts = jsonObject.getLong("ts");
                Integer vc = jsonObject.getInteger("vc");
                return new WaterSensor(id, ts, vc);
            }
        });
//        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
//                    @Override
//                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
//                        return element.getTs();
//                    }
//                });
//        SingleOutputStreamOperator<WaterSensor> streamOperator =
//                mapSource.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
//
//        Table table = tableEnv.fromDataStream(streamOperator, "id,ts,vc");
        Table table = tableEnv.fromDataStream(mapSource, "id,ts,vc");

        tableEnv.createTemporaryView("kafka01",table);
        tableEnv.sqlQuery("select * from kafka01").execute().print();
        //同步两张表




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
