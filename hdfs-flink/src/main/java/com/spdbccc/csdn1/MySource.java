package com.spdbccc.csdn1;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * @author ：wudl
 * @date ：Created in 2021-12-27 0:29
 * @description：
 * @modified By：
 * @version: 1.0
 */

public  class MySource implements SourceFunction<String> {
    private boolean isRunning = true;

    String[] citys = {"北京","广东","山东","江苏","河南","上海","河北","浙江","香港","山西","陕西","湖南","重庆","福建","天津","云南","四川","广西","安徽","海南","江西","湖北","山西","辽宁","内蒙古"};
    int i = 0;
    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        Random random = new Random();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        while (isRunning) {
            int number = random.nextInt(4) + 1;
            Integer id =  i++;
            String eventTime = df.format(new Date());
            String address = citys[random.nextInt(citys.length)];
            int productId = random.nextInt(25);
            ctx.collect(id+","+ address +","+ productId);
            Thread.sleep(500);
            //id int ,address string,int produceid
        }
    }
    @Override
    public void cancel() {

        isRunning = false;
    }


}
