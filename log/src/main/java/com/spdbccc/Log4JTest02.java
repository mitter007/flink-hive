package com.spdbccc;

import org.apache.log4j.*;

import java.io.IOException;

public class Log4JTest02 {
 
    public static void main(String[] args) {
        Logger logger = Logger.getLogger(Log4JTest02.class);
        BasicConfigurator.configure();
        HTMLLayout layout = new HTMLLayout();
        // SimpleLayout layout = new SimpleLayout();
        try {
            FileAppender appender = new FileAppender(layout, "D:\\out.html", false);
            logger.addAppender(appender);
            //设置日志输出级别为info，这将覆盖配置文件中设置的级别，只有日志级别高于WARN的日志才输出
            logger.setLevel(Level.WARN);
            logger.debug("这是debug");
            logger.info("这是info");
            logger.warn("这是warn");
            logger.error("这是error");
            logger.fatal("这是fatal");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
 
}