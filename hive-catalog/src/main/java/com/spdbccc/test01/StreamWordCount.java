package com.spdbccc.test01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public StreamWordCount() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> lineStream = env.socketTextStream("hadoop103", 7777);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = lineStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = line.split(" ");
                String[] var4 = words;
                int var5 = words.length;

                for(int var6 = 0; var6 < var5; ++var6) {
                    String word = var4[var6];
                    out.collect(Tuple2.of(word, 1L));
                }

            }
        }).keyBy((data) -> {
            return (String)data.f0;
        }).sum(1);
        sum.print("lineStream>>>");
        env.execute();
    }
}
