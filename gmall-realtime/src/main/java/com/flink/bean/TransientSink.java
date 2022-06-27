package com.flink.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author LR
 * @create 2022-06-27:10:54
 */

@Target(ElementType.FIELD)
@Retention(RUNTIME)
public @interface TransientSink {

}
