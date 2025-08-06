package com.spdbccc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: PulsarEvent
 * Package: com.spdbccc
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/22 11:07
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PulsarEvent {
    String  customerId;
    String  cardNumber;
    String  accountNumber;
    String  scenarioCode;
    String  eventCode;
    String  eventTime;
    String  activities;
    String  activityCode;
    String  childActivities;
    String  childActivityCode;
    String  activityRefuseCode;
    String  lables;
    String  lableCode;
    String  ruleExpStr;
    String  LableResult;
    String  errCode;
    String  errMsg;
}
