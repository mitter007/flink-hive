package com.spdbccc.bucket;

import com.spdbccc.bean.DataRow;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * ClassName: FileSinkBucketAssigner
 * Package: com.spdbccc.bucket
 * Description:
 *
 * @Author JWT
 * @Create 2025/8/5 20:32
 * @Version 1.0
 */
public class FileSinkBucketAssigner implements BucketAssigner<DataRow, String> {

    @Override
    public String getBucketId(DataRow element, Context context) {

        String hlrs_hdfs_dir = element.getTableName();
        String dt = element.getDt();
//        return new Path( File.separator + hlrs_hdfs_dir + File.separator + "dt=" + yyyyMMdd) ;
        return File.separator + hlrs_hdfs_dir + File.separator + "dt=" + dt;
    }

    @Override
    public SimpleVersionedSerializer getSerializer() {
        return null;
    }
}
