package com.flink.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author LR
 * @create 2022-06-27:9:44
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;

}
