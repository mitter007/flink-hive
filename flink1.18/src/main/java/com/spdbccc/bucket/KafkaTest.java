package com.spdbccc.bucket;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import com.spdbccc.bean.DataRow;
import com.spdbccc.bean.TableProcess;
import com.spdbccc.query.FlinkUtil;
import com.spdbccc.query.JdbcUtil;
import com.spdbccc.query.StringUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: HlrsKafka2HdfsApp
 * Package: com.spdbccc.hdfs.bucket
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/16 14:34
 * @Version 1.0
 */
public class KafkaTest {
//    hdfs://hadoop102:8020/

    //日志打印框架
    private static Logger logger = LoggerFactory.getLogger(KafkaTest.class);

    //默认的写输出根路径，具体每张表的分区输出路径在HlrsPathBucket中定义
    //输出到hdfs的路径的  从外部传参
    private static String ROOT_DIR = "hdfs://hadoop102:8020/spdccc/dfs/HLRS";


    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String flinkAppConfPath = parameterTool.get("flink_hdfs_config_path");

        System.out.println("配置路径为: " + flinkAppConfPath);
        //kafka.properties 加载配置文件
        ParameterTool propertistool = ParameterTool.fromPropertiesFile(flinkAppConfPath);


        KafkaTest hlrsKafka2HdfsApp = new KafkaTest();

        hlrsKafka2HdfsApp.runApp(propertistool);

    }

    //
    public void runApp(ParameterTool parameterTool) throws Exception {

        String TopicName = parameterTool.get("topic.name");
        String kafkaServers = parameterTool.get("kafka.servers");
        String groupID = parameterTool.get("group.id");
        String offset = parameterTool.get("kafka.default.offset");
        String root_dir = parameterTool.get("root_dir");
        //debug.log=true 这里是否写错了
        boolean logDebug = parameterTool.getBoolean("log.debug", true);
        logger.info("TopicName : {}", TopicName);
        logger.info("kafkaServers : {}", kafkaServers);
        logger.info("groupID :{} ", groupID);
        logger.info("offset : {}", offset);

        //获取flink的执行环境 设置checkpoint
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 10002);


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
//        1.12 及以后，flink 以 event time 作为默认的时间语义，并 deprecated 了上述设置 api；
//        @Deprecated
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(2, TimeUnit.MINUTES)));
        //设置checkpoint模式是精准一次
        // 每 5 分钟做一次 checkpoint
        env.enableCheckpointing(300 * 1000L);  // 毫秒单位

// 设置 checkpoint 模式（默认就是 EXACTLY_ONCE）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


        //kakfa
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaServers);
        properties.setProperty("group.id", groupID);
        properties.setProperty("auto.offset.reset", offset);
        //kerberos认证
 /*       properties.setProperty("security.protocol","SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism","GSSAPI");
        properties.setProperty("sasl.kerberos.service.name","kafka");*/

        //创建流 从kafka中读取数据

        //json数据
        // {"streamId":"7000","HLRS_HDFS_DIR":"hlrs_stream_7000","HLRS_ONCEFILTER":"false","HLRS_SYNC_COUNT":100,
        // "username":"testname1","locationdate":"202102020","other1","vsdv"}
        DataStreamSource<String> stream = env.fromSource(FlinkUtil.getKafkaSource(TopicName, groupID), WatermarkStrategy.noWatermarks(), "kafkaSource");
//
//        stream.assignTimestampsAndWatermarks((AssignerWithPeriodicWatermarks<String>) WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
//            @Override
//            public long extractTimestamp(JSONObject element, long recordTimestamp) {
//                return 0;
//            }
//        }))
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                return 0;
            }
        }));

        SingleOutputStreamOperator<JSONObject> JsonDS = stream.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return jsonObject;
            }
        });
        SingleOutputStreamOperator<JSONObject> watermarks = JsonDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        WindowedStream<JSONObject, String, TimeWindow> idDS = watermarks.keyBy(o -> o.getString("id")).window(SlidingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(30), org.apache.flink.streaming.api.windowing.time.Time.seconds(10))).allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(2));

        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("ss") {
        };
        idDS.sideOutputLateData(outputTag);




        env.execute();


    }


    /**
     * 根据输入的数据和redis参数，生成行数据
     * 第一列为other字段，用于保存未定义的字段
     *
     * @param inputStr
     * @param
     * @return
     */

    //  DataRow dataRow = bulidValue(s, redisProp);inputStr 就是传进来的json格式的文件


}
