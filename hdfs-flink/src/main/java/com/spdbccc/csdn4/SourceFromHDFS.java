package com.spdbccc.csdn4;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: SourceFromHDFS
 * Package: com.spdbccc
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/2 16:37
 * @Version 1.0
 */
public class SourceFromHDFS {
    public static void main(String[] args) throws Exception {
        //这种方法实际上是可以成功的
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //这里写成绝对路径的地址
        DataStreamSource<String> streamSource = env.readTextFile("hdfs://hadoop102:8020/1.txt");
        streamSource.print("hdfs>>>");
        env.execute();
    }
}
