package com.spdbccc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class MySqlSourceExample {

    public static void main(String[] args) throws Exception {

        String offsetFile = "binlog.000002";

        Long offsetPos = 160299739L;  //154 219 504
        Properties prop = new Properties();
        prop.setProperty("snapshot.locking.mode", "none");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("flink_config") // monitor all tables under inventory database
                .tableList("flink_config.t_user") // set captured table
                .username("root")
                .password("00000")

                //设置读取位置 initial全量, latest增量,  specificOffset(binlog指定位置开始读,该功能cdc2.2版本不支持)
                .startupOptions(StartupOptions.specificOffset(offsetFile, Long.valueOf(offsetPos)))
//                .startupOptions(StartupOptions.initial())
//                .startupOptions(StartupOptions.latest())
                .debeziumProperties(prop)
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print("====>")
                .setParallelism(1);

        env.execute();
    }
}
