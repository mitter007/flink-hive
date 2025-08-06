package com.spdbccc.bucket;


import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.joda.time.DateTime;

/**
 * ClassName: StreamidDayBucketAssigner
 * Package: com.spdbccc.hdfs.bucket
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/16 14:27
 * @Version 1.0
 */
public class StreamidDayBucketAssigner implements BucketAssigner<ObjectNode,String> {
    @Override
    public String getBucketId(ObjectNode jsonNodes, Context context) {
        String hlrs_hdfs_dir = jsonNodes.get("HLRS_HDFS_DIR").asText();
        DateTime dateTime = new DateTime();
        String yyyyMMdd = dateTime.toString("yyyyMMdd");

        return hlrs_hdfs_dir+"/dt="+yyyyMMdd;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return null;
    }
}
