package com.flink.common;

/**
 * @author LR
 * @create 2022-06-19:15:32
 */
public class GmallConfig {

    //Phoenix库名
    public static final String HASE_SCHEMA = "GMALL_REALTIME";
    // Phoenix驱动
    public static final String PHOENIX_DRIVER  = "org.apache.phoenix.jdbc.PhoenixDriver";
    // Phoenix 连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:bigdata01,bigdata02,bigdata03:2181";

    //ClickHouse_Url
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";

    //ClickHouse_Driver
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

}
