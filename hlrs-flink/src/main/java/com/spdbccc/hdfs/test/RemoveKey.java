package com.spdbccc.hdfs.test;

import com.alibaba.fastjson.JSONObject;

/**
 * ClassName: RemoveKey
 * Package: com.spdbccc.hdfs.test
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/17 16:51
 * @Version 1.0
 */
public class RemoveKey {

    public static void main(String[] args) {

        String value = "{\"streamId\":\"7000\",\"HLRS_HDFS_DIR\":\"hlrs_stream_7000\"," +
                "\"HLRS_ONCEFILTER\":\"false\",\"HLRS_SYNC_COUNT\":100,\"username\":\"testname1\"," +
                "\"locationdate\":\"202102020\",\"other1\":\"vsdv\"}";
        JSONObject inputJson = JSONObject.parseObject(value);
        String streamId = getAndRemove(inputJson,"streamId",null);
        System.out.println(streamId);
        // 7000  只获得某个key的value值


    }

    public static String getAndRemove(JSONObject inputJson, String key, String defaultStr){
        Object tempObj = inputJson.remove(key);

        return tempObj == null ? defaultStr : tempObj.toString();
    }
}
