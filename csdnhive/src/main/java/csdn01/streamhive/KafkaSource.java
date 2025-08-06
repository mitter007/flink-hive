package csdn01.streamhive;

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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * ClassName: KafkaSource
 * Package: csdn01.streamhive
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/15 15:20
 * @Version 1.0
 */
public class KafkaSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Configuration configuration = tableEnv.getConfig().getConfiguration();

        configuration.setString("table.exec.hive.fallback-mapred-reader", "true");
        //如果 topic 中的某些分区闲置，watermark 生成器将不会向前推进。 你可以在表配置中设置 'table.exec.source.idle-timeout' 选项来避免上述问题

        configuration.setString("table.exec.source.idle-timeout", "10s");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.reset","latest");
        //kerberos认证
 /*       properties.setProperty("security.protocol","SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism","GSSAPI");
        properties.setProperty("sasl.kerberos.service.name","kafka");*/

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

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDStream.assignTimestampsAndWatermarks(WatermarkStrategy
                //指定WaterMark
                .<WaterSensor>forMonotonousTimestamps()
                //分配事件时间戳
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                }));

        //怎么加上时间戳字段
        Table table = tableEnv.fromDataStream(waterSensorSingleOutputStreamOperator);
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        Table table1 = tableEnv.fromDataStream(rowDataStream);
        tableEnv.createTemporaryView("t1",table1);

//        tableEnv.executeSql("create table t2 ( id string, ts bigint, vc double)");
//        tableEnv.executeSql("insert into t2 select id,ts,vc from t1");  able options do not contain an option key 'connector' for discovering a connector.
        tableEnv.sqlQuery("select * from t1").execute().print();
    }
}
