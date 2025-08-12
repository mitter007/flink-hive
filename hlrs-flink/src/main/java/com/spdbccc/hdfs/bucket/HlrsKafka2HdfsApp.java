package com.spdbccc.hdfs.bucket;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.spdbccc.hdfs.DataRow;
import com.spdbccc.hdfs.TableConfig;
import com.spdbccc.hdfs.query.StringUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
public class HlrsKafka2HdfsApp {
//    hdfs://hadoop102:8020/

    //日志打印框架
    private static Logger logger = LoggerFactory.getLogger(HlrsKafka2HdfsApp.class);

    //默认的写输出根路径，具体每张表的分区输出路径在HlrsPathBucket中定义
    //输出到hdfs的路径的  从外部传参
    private static String ROOT_DIR ="hdfs://hadoop102:8020/spdccc/dfs/HLRS";

/*    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String flinkAppConfPath = parameterTool.get("flink_hdfs_config_path");

        //hlrs_query.properties 加载配置文件
        ParameterTool propertistool = ParameterTool.fromPropertiesFile(flinkAppConfPath);

        HlrsKafka2HdfsApp hlrsKafka2HdfsApp = new HlrsKafka2HdfsApp();

        hlrsKafka2HdfsApp.runApp(propertistool);
    }*/
public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String flinkAppConfPath = parameterTool.get("flink_hdfs_config_path");

    //hlrs_query.properties 加载配置文件
    ParameterTool propertistool = ParameterTool.fromPropertiesFile(flinkAppConfPath);

    HlrsKafka2HdfsApp hlrsKafka2HdfsApp = new HlrsKafka2HdfsApp();

    hlrsKafka2HdfsApp.runApp(propertistool);

}

    //
    public  void runApp(ParameterTool parameterTool) throws Exception {

        String TopicName = parameterTool.get("topic.name");
        String kafkaServers = parameterTool.get("kafka.servers");
        String groupID = parameterTool.get("group.id");
        String offset = parameterTool.get("kafka.default.offset");
            //debug.log=true 这里是否写错了
        boolean logDebug = parameterTool.getBoolean("log.debug",false);
        logger.info("TopicName : ",TopicName);
        logger.info("kafkaServers : ",kafkaServers);
        logger.info("groupID : ",groupID);
        logger.info("offset : ",offset);

        //获取flink的执行环境 设置checkpoint

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        1.12 及以后，flink 以 event time 作为默认的时间语义，并 deprecated 了上述设置 api；
//        @Deprecated
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
// 两次 checkpoint 的最小间隔时间，为了防止两次 checkpoint 的间隔时间太短
        env.getCheckpointConfig().setCheckpointInterval(2000);


        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(2, TimeUnit.MINUTES)));
        //设置checkpoint模式是精准一次
        env.enableCheckpointing(60*60*1000L, CheckpointingMode.EXACTLY_ONCE);


        //redis 解析redis的参数
        Properties redisProp = initRedisPorperties(parameterTool);

        //kakfa
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",kafkaServers);
        properties.setProperty("group.id", groupID);
        properties.setProperty("auto.offset.reset",offset);
        //kerberos认证
 /*       properties.setProperty("security.protocol","SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism","GSSAPI");
        properties.setProperty("sasl.kerberos.service.name","kafka");*/

        //创建流 从kafka中读取数据

        //json数据
       // {"streamId":"7000","HLRS_HDFS_DIR":"hlrs_stream_7000","HLRS_ONCEFILTER":"false","HLRS_SYNC_COUNT":100,
        // "username":"testname1","locationdate":"202102020","other1","vsdv"}
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>(TopicName, new SimpleStringSchema(), properties));
        //注册全局的properties
        env.getConfig().setGlobalJobParameters(parameterTool);


        //输入的json转换为DataRow
        SingleOutputStreamOperator<DataRow> map = stream.map(new MapFunction<String, DataRow>() {
            @Override
            public DataRow map(String s) throws Exception {
                try {
                    if (logDebug) {
                        logger.info("input str {}: ", s);
                    }

                    DataRow dataRow = bulidValue(s, redisProp);
                    return dataRow;
                } catch (Exception e) {
                    logger.error(e.getMessage() + s);
                    return new DataRow("", "", "");
                }
            }
        }).filter(new FilterFunction<DataRow>() {
            //过滤异常数据
            @Override
            public boolean filter(DataRow value) throws Exception {
                if (StringUtil.isEmpty(value.toString())) {
                    logger.info("---------datarow is null");
                    return false;
                } else {
                    return true;
                }
            }
        });
        BucketingSink<DataRow> sink = new BucketingSink<>(ROOT_DIR);
        sink.setBucketer(new HlrsPathBucket());
        sink.setWriter(new StringWriter<DataRow>());
        sink.setBatchRolloverInterval(60*60*1000L);
        sink.setBatchSize(10*1024*1024*1024);//文件大小10GB
        sink.setInactiveBucketThreshold(60*60*1000L);//设定不活动桶时间阈值，超过此值便关闭文件
        sink.setInactiveBucketCheckInterval(60*60*1000L);//设定检查不活动桶的频率


        sink.setInProgressPrefix("inProcessPre");//正在写入的文件后缀名
        sink.setPendingPrefix("pendingre");//挂起的文件，只有在checkpoint成功后才会变成完成状态

        sink.setPartPrefix("partPre");//完成状态文件

        //在这里使用了addsink 算子
        map.addSink(sink);
        env.execute();


    }


    /**
     * 根据输入的数据和redis参数，生成行数据
     * 第一列为other字段，用于保存未定义的字段
     * @param inputStr
     * @param properties
     * @return
     */

    //  DataRow dataRow = bulidValue(s, redisProp);inputStr 就是传进来的json格式的文件
    public static DataRow bulidValue(String inputStr,Properties properties){

        StringBuffer sb=new StringBuffer();
// {"streamId":"7000","HLRS_HDFS_DIR":"hlrs_stream_7000","HLRS_ONCEFILTER":"false","HLRS_SYNC_COUNT":100,
        // "username":"testname1","locationdate":"202102020","other1","vsdv"}
        //将string 转换为json格式
        JSONObject inputJson = JSONObject.parseObject(inputStr);
        String streamId = getAndRemove(inputJson,"streamId",null);
        String hlrs_hdfs_dir = getAndRemove(inputJson,"HLRS_HDFS_DIR",null); // 清除多余的字段
        getAndRemove(inputJson,"HLRS_ONCEFILTER",null);
        getAndRemove(inputJson,"HLRS_SYNC_COUNT",null);


        TableConfig.setRedisProperties(properties);
        //根据streamid就获得了整个tablecols
        JSONArray tablecols = TableConfig.getTablecols(streamId);
        for (int i = 0; i < tablecols.size(); i++) {
            String colName = tablecols.getJSONObject(i).getString("colName");

            String val =getAndRemove(inputJson,colName,"");
            String outVal = val.replaceAll("\\|",",").replaceAll("\n","").replaceAll("\r","");

            sb.append("|").append(outVal);

        }
        String otherStr  =inputJson.toJSONString().replaceAll("\\|",",").replaceAll("\n","").replaceAll("\r","");

        DateTime dateTime =new DateTime();
        String yyyyMMdd = dateTime.toString("yyyyMMdd");
        return new DataRow(otherStr+sb.toString(),yyyyMMdd,hlrs_hdfs_dir);

    }

    public static String getAndRemove(JSONObject inputJson,String key,String defaultStr){
        Object tempObj = inputJson.remove(key);

        return tempObj == null ? defaultStr : tempObj.toString();
    }

    private Properties initRedisPorperties(ParameterTool parameterTool){

        Properties redisprop = parameterTool.getProperties();
        return redisprop;
    }


}
