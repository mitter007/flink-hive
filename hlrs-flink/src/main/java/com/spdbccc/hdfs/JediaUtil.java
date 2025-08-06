package com.spdbccc.hdfs;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

import javax.xml.bind.PrintConversionEvent;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * ClassName: JediaUtil
 * Package: com.spdbccc.hdfs
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/17 17:09
 * @Version 1.0
 */
public class JediaUtil {
    /**
     * 缓存生存时间
     */
    //expire:到期
    private final int expire = 60000;
    private static Pool<Jedis> jedisPool = null;
    /**
     *根据配置文件不同，生成不同类型的pool，哨兵模式 or 集群模式
     */

    public static void bulidPool(Properties properties){
        if (jedisPool == null){
            jedisPool=buildSentinePool(properties);
        }
    }



    public static synchronized  Pool<Jedis>buildSentinePool(Properties properties){
        Pool<Jedis> jedisPool = null;
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(500);

        config.setMaxIdle(5);

        String maxIdle = properties.getProperty("sentinel.redis.maxIdle");
        if (maxIdle != null && maxIdle !=""){
            config.setMaxIdle(Integer.parseInt(maxIdle.trim()));

        }

        //这两行代码是什么意思
        config.setMaxWaitMillis(1000*100);
        String maxWait = properties.getProperty("sentinel.redis.maxWait");
        if (maxIdle != null && maxWait !=""){
            config.setMaxIdle(Integer.parseInt(maxWait.trim()));

        }
        String hosts =properties.getProperty("sentinel.redis.hosts");
        Set<String> hostSet = new HashSet<>();
        for (String ahost : hosts.split(",")) {
            hostSet.add(ahost);
        }

        String clusterName = properties.getProperty("sentinel.redis.clustername");

        String passwd = properties.getProperty("sentinel.redis.passwd");
         jedisPool= new JedisSentinelPool(clusterName,hostSet,passwd);
         return jedisPool;


    }

    /**
     * 从jedis连接池中获取jedis对象
     * @return
     */
    public static Jedis getJedis(){
        return jedisPool.getResource();
    }

    /**
     * 回收jedis（放到finally中）
     * @param jedis
     */

    public static void returnJedis(Jedis jedis){
        if (null != jedis){
            try {
                jedis.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
