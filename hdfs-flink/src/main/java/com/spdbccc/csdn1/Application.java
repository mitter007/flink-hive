package com.spdbccc.csdn1;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：wudl
 * @date ：Created in 2021-12-27 0:12
 * @description：
 * @modified By：
 * @version: 1.0
 */

//可以用这个代码改 kafkaSource
    //sink
    //Application
    //成功写出了文件
public class Application {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.addSource(new m)

///*        //设置检查点
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.10.102:8020/flink-checkpoint/checkpoint");
//        //设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms(为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了  --//默认是0
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//        //设置Checkpoint的时间间隔为1000ms做一次Checkpoint/其实就是每隔1000ms发一次Barrier!
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //设置同一时间有多少个checkpoint可以同时执行
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        // 设置重启策略
//        // 一个时间段内的最大失败次数
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
//                // 衡量失败次数的是时间段
//                Time.of(5, TimeUnit.MINUTES),
//                // 间隔
//                Time.of(10, TimeUnit.SECONDS)
//        ));*/
//        env.enableCheckpointing(10000L);
        // 启用检查点，间隔时间1秒
        env.enableCheckpointing(1000);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

// 设置精确一次模式
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// 最小间隔时间500毫秒
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
// 超时时间1分钟
        checkpointConfig.setCheckpointTimeout(60000);
// 同时只能有一个检查点
        checkpointConfig.setMaxConcurrentCheckpoints(1);
// 开启检查点的外部持久化保存，作业取消后依然保留
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
// 启用不对齐的检查点保存方式
        checkpointConfig.enableUnalignedCheckpoints();
// 设置检查点存储，可以直接传入一个String，指定文件系统的路径
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/flink-checkpoint/checkpoint"); //路径写对 checkpoint



        DataStreamSource<String> mySouceData = env.addSource(new MySource());
        mySouceData.print("mySourceData>>>>");
        FileSink<String> hdfsSink = HdfsSink.getHdfsSink();


        mySouceData.sinkTo((Sink<String, ?, ?, ?>) hdfsSink);
//        mySouceData.print("mySourceData");


        env.execute();

    }
}

