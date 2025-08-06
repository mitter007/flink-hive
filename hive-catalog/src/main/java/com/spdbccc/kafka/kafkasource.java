package com.spdbccc.kafka;

import com.alibaba.fastjson.JSONObject;
import com.spdbccc.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * ClassName: kafkasource
 * Package: com.spdbccc.kafka
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/14 9:12
 * @Version 1.0
 */
public class kafkasource {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
        Table table = tableEnv.fromDataStream(waterSensor, "id,ts,vc");
        tableEnv.createTemporaryView("t1",table);
        tableEnv.executeSql("select id,ts,vc from t1").print();

        env.execute();


    }
}
