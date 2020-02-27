package com.kafka.controller;

import com.kafka.service.Sender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @创建人 sgwang
 * @name SendController
 * @user 91119
 * @创建时间 2020/2/11
 * @描述
 */
@RestController
@RequestMapping("/send")
public class SendController {

    @Autowired
    private Sender sender;

    @RequestMapping(value = "")
    public String send() {
        sender.send("send");
        return "{\"code\":0}";
    }

    @RequestMapping(value = "get")
    public String sendByGet() {
        sender.sendByGet("sendByGet");
        return "{\"code\":0}";
    }

    @RequestMapping(value = "future")
    public String sendByFuture() {
        sender.sendByFuture("sendByFuture");
        return "{\"code\":0}";
    }

    @RequestMapping(value = "/part")
    public String sendToMyPartitioner() {
        sender.sendToMyPartitioner("sendToMyPartitioner");
        return "{\"code\":0}";
    }

    @RequestMapping(value = "/ack")
    public String sendToAck() {
        sender.sendToAck("sendToAck");
        return "{\"code\":0}";
    }

}
