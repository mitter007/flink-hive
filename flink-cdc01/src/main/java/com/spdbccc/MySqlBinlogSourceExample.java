package com.spdbccc;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

public class MySqlBinlogSourceExample {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(3000L);
    MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("hadoop102")
            .port(3306)
            .databaseList("flink_config") // set captured database
            .tableList("flink_config.t_user") // set captured table
            .username("root")
            .password("000000")
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .startupOptions(StartupOptions.initial())
            .build();



    // enable checkpoint

    env
            .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
            // set 4 parallel source tasks
            .setParallelism(4)
            .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

    env.execute("Print MySQL Snapshot + Binlog");
  }
}