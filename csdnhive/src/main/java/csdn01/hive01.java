package csdn01;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * ClassName: hive01
 * Package: com.spdbccc.csdnhive01
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/13 22:41
 * @Version 1.0
 */
public class hive01 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);




        String name            = "myhive";
        String defaultDatabase = "catalog";
        String hiveConfDir     = "D:\\code\\workspace_exer\\flink-hive\\hive-catalog\\src\\main\\resources\\";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);

// 设置 HiveCatalog 为会话的当前 catalog
        tableEnv.useCatalog("myhive");
        tableEnv.executeSql("show databases").print();

    }
}
