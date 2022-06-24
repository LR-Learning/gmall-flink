package com.flink.bean.function;

import com.alibaba.fastjson.JSONObject;
import com.flink.common.GmallConfig;

import com.flink.utils.DimUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author LR
 * @create 2022-06-20:18:57
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;


    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }


    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;

        try {
            // 获取SQL语句
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = genUpsertSql(sinkTable, after);
            System.out.println(upsertSql);
            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            // 判断如果当前数据为更新操作,则先删除Redis中的数据
            DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
            if ("update".equals(value.getString("type"))){
                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
            }
            // 执行插入操作
            preparedStatement.executeUpdate();
        } catch (SQLException e){
            e.printStackTrace();
        }finally {
            if (preparedStatement != null){
                preparedStatement.close();
            }
        }

    }

    private String genUpsertSql(String sinkTable, JSONObject data) {

        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();



        return "upsert into" + GmallConfig.HASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keySet, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";

    }


}
