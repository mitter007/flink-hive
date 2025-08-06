package com.spdbccc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: FlinkCDC_02
 * Package: com.spdbccc.flinkcdc
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/18 10:27
 * @Version 1.0
 */
public class FlinkCDC_02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("flink_config") // set captured database
                .tableList() // set captured table
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .startupOptions(StartupOptions.initial())  Incremental snapshot for tables requires primary key initial
                .startupOptions(StartupOptions.latest())
                .build();

        // TODO 4.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS =
                env.fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "MysqlSource");

        // TODO 5.打印输出
        mysqlDS.print("mysqlSource");

        // TODO 6.执行任务
        env.execute();
    }
}
