package com.spdbccc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: FlinkCDCTest
 * Package: com.spdbccc
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/18 14:14
 * @Version 1.0
 */

//{"before":null,"after":{"id":1011,"name":"leijun"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1692339317000,"snapshot":"false","db":"flink_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000097","pos":5997,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1692339317439,"transaction":null}

public class FlinkCDCTest {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flink-1109");
//
//        System.setProperty("HADOOP_USER_NAME", "atguigu");


//        {"before":null,"after":{"id":1010,"name":"leijun"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1692338600000,"snapshot":"false","db":"flink_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000097","pos":5691,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1692338600036,"transaction":null}
        //TODO 2.构建MySQLCDC的Source
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("hadoop102")
//                .port(3306)
//                .username("root")
//                .password("000000")
//                .databaseList("flink_config")
//                .tableList("flink_config.t_user")
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                .startupOptions(StartupOptions.initial())
//                .build();
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("flink_config")
                .tableList("flink_config.t_user")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> streamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");

        //TODO 3.打印
        streamSource.print();

        //TODO 4.启动任务
        env.execute();
    }
}
