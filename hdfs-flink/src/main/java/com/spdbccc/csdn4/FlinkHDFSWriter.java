package com.spdbccc.csdn4;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;

public class FlinkHDFSWriter {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 数据源
        env.fromElements(new Tuple2<>("foo", 1), new Tuple2<>("bar", 2), new Tuple2<>("baz", 3))
                .map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> value) throws Exception {
                        return value.f0 + "," + value.f1;
                    }
                })
                // 写入HDFS
                .writeAsText("hdfs://hadoop102:8020/output", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("Flink HDFS Writer");
    }
}
