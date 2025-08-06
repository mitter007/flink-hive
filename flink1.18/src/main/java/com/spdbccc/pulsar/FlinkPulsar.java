package com.spdbccc.pulsar;

import com.spdbccc.query.FlinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: FlinkPulsar
 * Package: com.spdbccc.pulsar
 * Description:
 *
 * @Author JWT
 * @Create 2025/8/5 23:11
 * @Version 1.0
 */
public class FlinkPulsar {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.fromSource(FlinkUtil.getPulsarSource(), WatermarkStrategy.noWatermarks(), "sss");
        source.print();
        env.execute();
    }
}
