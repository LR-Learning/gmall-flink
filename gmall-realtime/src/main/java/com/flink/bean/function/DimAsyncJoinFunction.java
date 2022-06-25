package com.flink.bean.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author LR
 * @create 2022-06-25:10:57
 */
public interface DimAsyncJoinFunction<T> {
    String getKey(T input);

    void join(T input, JSONObject dimInfo) throws ParseException;

}
