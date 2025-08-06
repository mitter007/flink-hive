package csdn01.kafkahive01;

import com.alibaba.fastjson.JSONObject;

import csdn01.bean.WaterSensor;
import lombok.val;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
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
//        kafkaStream.print();

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

        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((WaterSensor, timestamp) -> Long.parseLong(WaterSensor.getId()));
        SingleOutputStreamOperator<WaterSensor> watermarks = waterSensor.assignTimestampsAndWatermarks(watermarkStrategy);

        // 将DataStream转换为表，并添加处理时间时间戳字段
        tableEnv.createTemporaryView("myTable", watermarks, "id, ts, vc, pt.proctime");

        // 查询表并输出结果
//        Table result = tableEnv.sqlQuery("SELECT * FROM myTable");
//        tableEnv.toAppendStream(result, Row.class).print();
      tableEnv.executeSql("select * from myTable").print();


        // 执行任务
        env.execute("Stream to Table with Timestamp Example");


        env.execute();


    }
}
