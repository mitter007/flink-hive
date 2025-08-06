//package com.spdbccc.test;
//
//import org.apache.flink.table.catalog.hive.HiveCatalog;
//
///**
// * ClassName: CatalogUtil
// * Package: com.spdbccc
// * Description:
// *
// * @Author 焦文涛
// * @Create 2023/8/23 10:06
// * @Version 1.0
// */
//public class CatalogUtil {
//        public static HiveCatalog  getHiveCatalog(String name,String defaultDatabase,String  hiveConfDir){
//            // 1.创建HiveCatalog
////            String name = "myhive";
////            String defaultDatabase = "catalog";
////            String hiveConfDir = "D:\\code\\pulsar-hive01\\hive-catalog\\src\\main\\resources\\";
//
//            HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
//            // 2.注册HiveCatalog
////            tableEnv.registerCatalog(name, hive);
////            // 3.把HiveCatalog: myhive作为当前session的catalog
////            tableEnv.useCatalog(name);
////            tableEnv.useDatabase(defaultDatabase);
////            //指定方言
////            tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//            return hive;
//
//
//        }
//}
