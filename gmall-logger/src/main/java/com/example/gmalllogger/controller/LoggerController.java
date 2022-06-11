package com.example.gmalllogger.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author LR
 * @create 2022-06-11:22:12
 */
@RestController
public class LoggerController {
    @RequestMapping("test1")

    public String test1(){
        System.out.println("success");
        return "success";
    }

}
