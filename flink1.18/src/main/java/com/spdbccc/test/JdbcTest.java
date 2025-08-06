package com.spdbccc.test;

import com.spdbccc.bean.TableProcess;
import com.spdbccc.query.JdbcUtil;
import com.spdbccc.query.JdbcUtilList;

import java.util.List;

/**
 * ClassName: JdbcTest
 * Package: com.spdbccc.test
 * Description:
 *
 * @Author JWT
 * @Create 2025/8/5 17:11
 * @Version 1.0
 */
public class JdbcTest {
    public static void main(String[] args) throws Exception {
        TableProcess query = JdbcUtil.query(JdbcUtil.getMySQLConnection(), "select * from leet.table_process", TableProcess.class, true);
        System.out.println(query);
    }
}
