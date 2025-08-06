package csdn01.kafkahive01;

import com.alibaba.fastjson.JSONObject;
import csdn01.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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



//pt 是时间戳字段2023-08-15 16:22:38.560
//+----+--------------------------------+----------------------+-------------+-------------------------+
//| op |                             id |                   ts |          vc |                      pt |
//+----+--------------------------------+----------------------+-------------+-------------------------+
//| +I |                             27 |            100000000 |          18 | 2023-08-15 16:22:38.560 |
//| +I |                             27 |            100000000 |          18 | 2023-08-15 16:22:41.502 |
//| +I |                             27 |            100000000 |          18 | 2023-08-15 16:22:43.427 |
//| +I |                             27 |            100000000 |          18 | 2023-08-15 16:22:45.321 |
public class kafkasource01 {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("" +
                "CREATE TABLE t1_pt(\n" +
                "    id string,\n" +
                "    ts bigint,\n" +
                "    vc int,\n" +
                "    pt AS PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'test1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'sink.partitioner' = 'fixed',\n" +
                "  'topic' = 'csdn02',\n" +
                "  'format' = 'json'\n" +
                ")");
        tableEnv.executeSql("select * from t1_pt").print();


    }
}
