package csdn01.kafkahive01;

import csdn01.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: Kafkahive06
 * Package: csdn01.kafkahive01
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/15 11:20
 * @Version 1.0
 */

//打印出来看看和kafka打印出来的流有什么不同
//+----+--------------------------------+----------------------+-------------+
//| op |                             id |                   ts |          vc |
//+----+--------------------------------+----------------------+-------------+
//| +I |                           1001 |                   12 |           2 |
//| +I |                           1002 |                   13 |           3 |
//| +I |                           1003 |                   14 |           3 |
//+----+--------------------------------+----------------------+-------------+
//没什么不同一样的
public class Kafkahive06 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<WaterSensor> streamSource = env.fromElements(new WaterSensor("1001", 12L, 2), new WaterSensor("1002", 13L, 3), new WaterSensor("1003", 14L,
                3));
        Table table = tableEnv.fromDataStream(streamSource, "id,ts,vc");
        tableEnv.createTemporaryView("kafka01",table);
        tableEnv.sqlQuery("select * from kafka01").execute().print();
    }
}
