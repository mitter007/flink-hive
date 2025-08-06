/*
package com.spdbccc.csdn4;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class WriteToHadoopExample {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 创建一个输入数据集
        // 这里使用Tuple2作为示例，根据实际情况修改
        DataSet<Tuple2<String, Integer>> inputDataSet = env.fromElements(
                new Tuple2<>("A", 1),
                new Tuple2<>("B", 2),
                new Tuple2<>("C", 3)
        );

        // 将数据写入Hadoop
        inputDataSet
                .map(new MapFunction<Tuple2<String, Integer>, Tuple2<NullWritable, Tuple2<String, Integer>>>() {
                    @Override
                    public Tuple2<NullWritable, Tuple2<String, Integer>> map(Tuple2<String, Integer> value) throws Exception {
                        return Tuple2.of(NullWritable.get(), value);
                    }
                })
                .output(new HadoopOutputFormat<>(new MyOutputFormat(), new JobConf()));

        // 执行作业
        env.execute("Write to Hadoop Example");
    }

    // 自定义OutputFormat
    public static class MyOutputFormat extends org.apache.hadoop.mapred.FileOutputFormat<NullWritable, Tuple2<String, Integer>> {
        @Override
        public org.apache.hadoop.mapred.RecordWriter<NullWritable, Tuple2<String, Integer>> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, org.apache.hadoop.util.Progressable progressable) throws IOException {
            // 自定义输出逻辑
            org.apache.hadoop.mapred.RecordWriter<NullWritable, Tuple2<String, Integer>> recordWriter = new org.apache.hadoop.mapred.RecordWriter<NullWritable, Tuple2<String, Integer>>() {
                @Override
                public void write(NullWritable nullWritable, Tuple2<String, Integer> tuple2) throws IOException {
                    // 写入逻辑
                }

                @Override
                public void close(Reporter reporter) throws IOException {
                    // 关闭逻辑
                }
            };

            return recordWriter;
        }
    }
}
*/
