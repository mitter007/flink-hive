package com.spdbccc.hdfs;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.StatisticalMultivariateSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import scala.runtime.VolatileDoubleRef;

import javax.xml.bind.PrintConversionEvent;
import java.util.Properties;

/**
 * ClassName: TableConfig
 * Package: com.spdbccc.hdfs
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/16 16:25
 * @Version 1.0
 */

/**
 * 持久化配置表相关的配置
 */
public class TableConfig {
    private static Logger logger = LoggerFactory.getLogger(TableConfig.class);
    private static Long LastConfigTimestamp = 0L;
    private static String LastConfigStr = null;
    private static Properties redisProperties;
    private final static String PERSIST_CONFIG = "CDH_PERSIST_CONFIG";


    public static void setRedisProperties(Properties prop){
        redisProperties = prop;
    }


    //这里是获取列名？
    //从这里就获取了columns
    public static JSONArray getTablecols(String StreamId){

        long currentTimeMillis = System.currentTimeMillis();

        if (StringUtils.isEmpty(LastConfigStr) || (currentTimeMillis - LastConfigTimestamp >= 60*1000L)){
//            reflash

            LastConfigTimestamp= System.currentTimeMillis();
        }

        try {
            JSONObject rootJson = JSONObject.parseObject(LastConfigStr);
            JSONObject tableMap = rootJson.getJSONObject("tableMap");
            JSONObject aStream = tableMap.getJSONObject(StreamId);
            if (aStream == null){
                //将异常输出到日志
                logger.error("数据源id未注册为持久化表 {}",LastConfigStr);
                return null;
            }
            //获取列名
            JSONArray jsonArray = aStream.getJSONArray("colums");
            return jsonArray;
        } catch (Exception e) {
            throw new RuntimeException(String.format("获取列名异常 %s",LastConfigStr));
        }
    }

    //刷新


    private static void reflash(){

        JediaUtil.bulidPool(redisProperties);
        Jedis jedis = JediaUtil.getJedis();
        try {
            //常量大写
            String s = jedis.get(PERSIST_CONFIG);

            if (StringUtils.isEmpty(s)){
                //update persist_config fatle:empty persist_config string from redis
                logger.warn("UPDATE PERSIST_CONFIG FATLE:empty PERSIST_CONFIG string from redis");

            }
            //如果不为空则更新程序内缓存的持久化表信息
            //更新程序内缓存的持久化表信息
            LastConfigStr = s;

            // update persist_config{}
            logger.info("UPDATE PERSIST_CONFIG {}",s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            //关闭jedis jedis.close
            }

    }



}
