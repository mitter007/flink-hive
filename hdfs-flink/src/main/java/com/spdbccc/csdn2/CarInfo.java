package com.spdbccc.csdn2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: CarInfo
 * Package: com.csdn2
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/2 22:21
 * @Version 1.0
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CarInfo {
    String CarNo;
    Double Speed;
    Long Timestamp;

}
