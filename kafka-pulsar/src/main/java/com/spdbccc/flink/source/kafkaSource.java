package com.spdbccc.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * ClassName: kafkaSource
 * Package: com.spdbccc.flink.source
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/11 8:57
 * @Version 1.0
 */
public class kafkaSource implements SourceFunction<String> {
    @Override
    public void run(SourceContext<String> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
