package com.spdbccc.hdfs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: DataROw
 * Package: com.spdbccc.hdfs
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/16 14:08
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataRow {
    String val;
    String dt;
    String tableName;

    @Override
    public String toString() {
        return val;
    }
}
