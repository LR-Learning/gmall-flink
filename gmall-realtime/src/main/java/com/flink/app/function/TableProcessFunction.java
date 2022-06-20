package com.flink.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.bean.TableProcess;
import com.flink.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author LR
 * @create 2022-06-19:15:27
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.HASE_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.HASE_SERVER);
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        // 主流
        // 1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            // 2.过滤字段
            JSONObject data = value.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());

            // 3.分流
            // 将输出表/主题信息写入value
            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                // kafka数据，写入主流
                collector.collect(value);
            }else if (TableProcess.SINK_TYPE_HASE.equals(sinkType)){
                // Hase数据，写入侧输出流
                readOnlyContext.output(objectOutputTag, value);
            }
        } else {
            System.out.println("该组合：" + key + "不存在！");
        }


    }

    /**
     *
     * @param data  {"id":"11,"tn_name":"name", "logo_url":"aaa"}
     * @param sinkColumns id, tm_name
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields); // 将数组转为集合

//        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columns.contains((next.getKey()))){
//                iterator.remove();
//            }
//        }

        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));
    }

    // value:{"db":"","tn":"","before":"","after":"","type":""}
    @Override
    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
        // 广播流
        // 1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(s);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        // 2.建表
        if (TableProcess.SINK_TYPE_HASE.equals(tableProcess.getSinkType())){
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPK(),
                    tableProcess.getSinkExtend());
        }

        // 3.写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    // 建表语句: create table if not exists db.tn(id varchar primary key, tm_name varchar) xxxx;
    private void checkTable(String sinkTable, String sinkColumns, String sinkPK, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            if (sinkPK == null){
                sinkPK = "id";
            }
            if (sinkExtend == null){
                sinkExtend = "";
            }

            StringBuffer createTableSQL = new StringBuffer("create table if not exists")
                    .append(GmallConfig.HASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] fields = sinkColumns.split(",");
            for (int i = 0; i < fields.length; i++) {

                String field = fields[i];
                // 判断是否为主键
                if (sinkPK.equals(field)) {
                    createTableSQL.append(" varchar primary key");
                } else {
                    createTableSQL.append(" varchar");
                }

                // 判断是否为最后一个字段,如果不是,则添加","
                if (i < fields.length - 1) {
                    createTableSQL.append(",");
                }
            }
            createTableSQL.append(")").append(sinkExtend);

            // 打印建表语句
            System.out.println(createTableSQL);

            // 预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败");
        } finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
    }

}
