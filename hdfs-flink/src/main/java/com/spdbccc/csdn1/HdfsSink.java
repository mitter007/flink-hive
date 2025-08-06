package com.spdbccc.csdn1;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * @author ：wudl
 * @date ：Created in 2021-12-26 23:49
 * @description：HdfsSink
 * @modified By：
 * @version: 1.0
 */

public class HdfsSink {
    public static FileSink<String> getHdfsSink() {
        //写出的文件名称
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("test")
                .withPartSuffix(".txt")
                .build();

//        FileSink<String> finkSink = FileSink.forRowFormat(new Path("hdfs://hadoop102:8020/FlinkFileSink/"),
//                        new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(DefaultRollingPolicy.builder()
////                        每隔15分钟生成一个新文件
//                        .withRolloverInterval(TimeUnit.SECONDS.toMillis(10))
//                        //每隔5分钟没有新数据到来,也把之前的生成一个新文件
//                        .withInactivityInterval(TimeUnit.SECONDS.toMillis(5))
//                        .withMaxPartSize(1024 * 1024 * 1024)
//                        .build())
//
//
////        分桶策略
//                .withOutputFileConfig(config)
//                .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd"))
//                .build();

        FileSink fileSink = FileSink.forRowFormat(new Path("hdfs://hadoop102:8020/FlinkFileSink/wudlfile"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()

                        //10秒生成一个文件
                        .withRolloverInterval(TimeUnit.SECONDS.toMillis(10))
                        //间隔多久
                        .withInactivityInterval(TimeUnit.SECONDS.toMillis(5))
                        //文件最大大小
                        .withMaxPartSize(1024 * 1024).build()
                ).withOutputFileConfig(config)
                .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd", ZoneId.of("Asia/Shanghai")))
                .build();

        return  fileSink;
    }
}

