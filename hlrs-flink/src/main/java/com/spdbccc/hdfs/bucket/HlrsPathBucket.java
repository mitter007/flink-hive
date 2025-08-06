package com.spdbccc.hdfs.bucket;

import com.spdbccc.hdfs.DataRow;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.hadoop.fs.Path;

import java.io.File;

/**
 * ClassName: HlrsPathBucket
 * Package: com.spdbccc.hdfs.bucket
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/16 14:04
 * @Version 1.0
 */
public class HlrsPathBucket  extends BasePathBucketer<DataRow> {
    @Override
    public Path getBucketPath(Clock clock, Path basePath, DataRow element) {

        String hlrs_hdfs_dir=element.getTableName();
        String dt = element.getDt();
        return new Path(basePath + File.separator+hlrs_hdfs_dir+File.separator+"dt="+dt);
    }
}
