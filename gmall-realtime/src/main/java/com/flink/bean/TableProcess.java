package com.flink.bean;

import lombok.Data;

/**
 * @author LR
 * @create 2022-06-19:15:02
 */
@Data
public class TableProcess {

    // 动态分流Sink常量
    public static final String SINK_TYPE_HASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";

    // 来源表
    String sourceTable;
    // 操作类型 insert,update,delete
    String operateType;
    // 输出类型 hase,kafka
    String sinkType;
    // 输出表(主题)
    String sinkTable;
    // 输出字段
    String sinkColumns;
    // 主题字段
    String sinkPK;
    // 见表扩展
    String sinkExtend;

}
