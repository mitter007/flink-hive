package csdn01.chat01;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class StreamToTableWithTimestampExample {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建数据流
        DataStream<Row> stream = env.fromElements(
                Row.of("27", 27L, 18),
                Row.of("28", 28L, 20),
                Row.of("29", 29L, 15)
        );

        // 定义Watermark策略
        WatermarkStrategy<Row> watermarkStrategy = WatermarkStrategy
                .<Row>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((row, timestamp) -> (long) row.getField(1));

        // 转换为带有时间戳的DataStream
        DataStream<Row> timestampedStream = stream
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 将DataStream转换为表，并添加处理时间时间戳字段
        tEnv.createTemporaryView("myTable", timestampedStream, "id, ts, vc, pt.proctime");

        // 查询表并输出结果
        Table result = tEnv.sqlQuery("SELECT * FROM myTable");
        tEnv.toAppendStream(result, Row.class).print();

        // 执行任务
        env.execute("Stream to Table with Timestamp Example");
    }
}
