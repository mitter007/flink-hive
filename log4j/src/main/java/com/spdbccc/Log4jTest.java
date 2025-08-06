package com.spdbccc;

import org.apache.log4j.*;
import org.junit.Test;

import java.io.PrintWriter;

/**
 * ClassName: tes
 * Package: com.spdbccc
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/20 17:54
 * @Version 1.0
 */
public class Log4jTest {
    @Test
    public void testLogger() {
        Logger logger = Logger.getLogger(Log4jTest.class);
        BasicConfigurator.configure();

        // 日志记录输出
        logger.info("hello log4j");
        // 日志级别
        logger.fatal("fatal"); // 严重错误，一般会造成系统崩溃和终止运行
        logger.error("error"); // 错误信息，但不会影响系统运行
        logger.warn("warn"); // 警告信息，可能会发生问题
        logger.info("info"); // 程序运行信息，数据库的连接、网络、IO操作等
        logger.debug("debug"); // 调试信息，一般在开发阶段使用，记录程序的变量、参数等
        logger.trace("trace"); // 追踪信息，记录程序的所有流程信息
        // 配置一个控制台输出源
        ConsoleAppender consoleAppender = new ConsoleAppender();
        consoleAppender.setName("ydl");
        consoleAppender.setWriter(new PrintWriter(System.out));
        logger.addAppender(consoleAppender);

    }
    @Test
    public void testLog(){
        // 获取一个logger
        Logger logger = Logger.getLogger(Log4jTest.class);
        // 创建一个layout
        Layout layout = new PatternLayout("%-d{yyyy-MM-dd HH:mm:ss} [%t:%r] -[%p] %m%n");
        // 创建一个输出源
        ConsoleAppender appender = new ConsoleAppender();
        appender.setLayout(layout);
        appender.setWriter(new PrintWriter(System.out));
        logger.addAppender(appender);
        logger.warn("warning");
    }
    @Test
    public void testConfig(){
        // 获取一个logger
        Logger logger = Logger.getLogger(Log4jTest.class);
        logger.warn("warning");
    }


}
