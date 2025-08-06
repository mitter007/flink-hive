package com.spdbccc.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * ClassName: CataTest01
 * Package: com.spdbccc
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/23 10:12
 * @Version 1.0
 */
public class CataTest01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        HiveCatalog hive = CatalogUtil.getHiveCatalog("myhive", "catalog", "D:\\\\code\\\\pulsar-hive01\\\\hive-catalog\\\\src\\\\main" +
                "\\\\resources");

        tableEnv.registerCatalog("myhive", hive);
        // 3.把HiveCatalog: myhive作为当前session的catalog
        tableEnv.useCatalog("myhive");
        tableEnv.useDatabase("catalog");
        //指定方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        tableEnv.executeSql("create table if not exists t2(id int ,name string)").print();
//        tableEnv.executeSql("insert into t2 values(1001,'a')");
        tableEnv.executeSql("select * from t2").print();
    }
}
