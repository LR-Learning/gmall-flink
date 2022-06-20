package com.flink.bean;

/**
 * @author LR
 * @create 2022-06-19:15:02
 */
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

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getOperateType() {
        return operateType;
    }

    public void setOperateType(String operateType) {
        this.operateType = operateType;
    }

    public String getSinkType() {
        return sinkType;
    }

    public void setSinkType(String sinkType) {
        this.sinkType = sinkType;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public String getSinkColumns() {
        return sinkColumns;
    }

    public void setSinkColumns(String sinkColumns) {
        this.sinkColumns = sinkColumns;
    }

    public String getSinkPK() {
        return sinkPK;
    }

    public void setSinkPK(String sinkPK) {
        this.sinkPK = sinkPK;
    }

    public String getSinkExtend() {
        return sinkExtend;
    }

    public void setSinkExtend(String sinkExtend) {
        this.sinkExtend = sinkExtend;
    }
}
