package com.spdbccc;

import org.apache.log4j.Logger;

public class Log4JTest03 {
 
    public static void main(String[] args) {


        //获取Logger对象的实例         
        Logger logger = Logger.getLogger(Log4JTest03.class); //这步有问题
        logger.debug("这是debug");
        logger.info("这是info");
        logger.warn("这是warn");
        logger.error("这是error");
        logger.fatal("这是fatal");
    }
 
}