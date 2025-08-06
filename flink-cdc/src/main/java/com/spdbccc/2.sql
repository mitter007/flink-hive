CREATE TABLE ccwn_zh_event_push (
customerid string,
cardnumber string,
accountnumber string,
eventcode string,
eventtime string,activities string,
activityRefuseCode string,
lables string)
PARTITIONED BY(dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive: ql.io.parquet.serde.ParquetHiveSerDe'
    WITH SERDEPROPERTIES ('fielddelim'='|','serialization.format'='|')
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    0UTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquet0utputFormat'
    LOCATION 'hdfs://nameservicel/spdbccc/dfs/CCWN/DEY CCWN/ccwn_zh_event_push'