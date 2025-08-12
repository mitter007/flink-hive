package com.spdbccc.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.naming.Name;

/**
 * ClassName: KafkaBean
 * Package: com.spdbccc.kafka
 * Description:
 *
 * @Author JWT
 * @Create 2025/8/7 11:46
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaBean {
    Integer id;
    String name;
    String email;
    Integer phone;
}
