package com.flink.utils;


import com.alibaba.fastjson.JSONObject;
import com.flink.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author LR
 * @create 2022-06-24:14:30
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {

        // 查询Phoenix之前先查询redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null){
            // 归还连接
            jedis.close();

            // 重置过期时间
            jedis.expire(redisKey, 24*60*60);
            // 返回结果
            return JSONObject.parseObject(dimInfoJsonStr);
        }

        // 拼接查询语句 select * from db.tn where id ='18';
        String querySql = "select * from " + GmallConfig.HASE_SCHEMA + "." + tableName +
                " where id='" + id + "'";
        // 查询Phoenix
        List<JSONObject> queryList = JDBCUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfoJson = queryList.get(0);
        // 在返回结果之前,将数据写入redis
        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24*60*60);
        jedis.close();
        // 返回结果
        return dimInfoJson;
    }

    public static void delRedisDimInfo(String tableName, String id){
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

}
