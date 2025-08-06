package csdn01.chat01;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class StreamToTableWithTimestamp {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建数据流
        DataStream<Row> stream = env.fromElements(
                Row.of("A", 1L),
                Row.of("B", 2L),
                Row.of("C", 3L)
        );

        // 分配时间戳和水位线
        DataStream<Row> streamWithTimestamps = stream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {
            private long currentMaxTimestamp = 0L;
            private final long maxOutOfOrderness = 1000L;

            @Override
            public long extractTimestamp(Row element, long previousElementTimestamp) {
                long timestamp = (long) element.getField(1);
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }
        });

        // 将流转换为表
        Table table = tEnv.fromDataStream(streamWithTimestamps, "name, timestamp.rowtime");

        // 打印表的内容
        table.printSchema();
        tEnv.toAppendStream(table, Row.class).print();

        // 执行作业
        env.execute("Stream to Table with Timestamp");
    }
}
