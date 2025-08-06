CREATE TABLE ccwn_zh_event_push (
                                    customerid string,
                                    cardnumber string,
                                    accountnumber string,
                                    eventcode string,
                                    eventtime string,activities string,
                                    activityRefuseCode string,
                                    lables string)
    PARTITIONED BY(dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    WITH SERDEPROPERTIES ('fielddelim'='|','serialization.format'='|')
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    0UTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquet0utputFormat'
    LOCATION 'hdfs://user/ccwn_zh_event_push'




DROP TABLE IF EXISTS ods_log_inc;
CREATE EXTERNAL TABLE ods_log_inc
(
    `common`   STRUCT<ar :STRING,
    ba :STRING,ch :STRING,
    is_new :STRING,
    `ts`       BIGINT  COMMENT '时间戳'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_log_inc/';