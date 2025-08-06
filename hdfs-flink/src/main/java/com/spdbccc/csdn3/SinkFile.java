package com.spdbccc.csdn3;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;


//这个是测试成功了 写出到了flink
public class SinkFile {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.fromElements("hello world", "hello flink");


//        env.fromElements()
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("hdfs://192.168.10.102:8020/output"),
                new SimpleStringEncoder<String>("UTF-8"))
                        .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                            .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                            .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                            .withMaxPartSize(1024 * 1024 * 1024)
                            .build())
                .build();

        input.addSink(sink);

        env.execute();
    }
}
