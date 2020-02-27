package com.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.Resource;
import java.util.concurrent.ExecutionException;

/**
 * @创建人 sgwang
 * @name Sender
 * @user 91119
 * @创建时间 2020/2/11
 * @描述
 */
@Service
public class Sender {

    private static final Logger logger = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Resource(name = "part_kafka_template")
    private KafkaTemplate<String, Object> simpleKafkaTemplate;

    @Resource(name = "ack_kafka_template")
    private KafkaTemplate<String, Object> ackKafkaTemplate;

    @Value("${app.topic}")
    private String topic;

    // 发送即可
    public void send(String message) {
        logger.info("------------------------ 消息发送Start -------------------");
        logger.info("Sender： Message:" + message + "   Topic:" + topic);

        kafkaTemplate.send(topic, message);

        logger.info("------------------------ 消息发送End -------------------");
    }

    // 同步发送
    public void sendByGet(String message) {
        logger.info("------------------------ 消息发送Start -------------------");
        logger.info("sendByGet： Message:" + message + "   Topic:" + topic);

        try {
            kafkaTemplate.send(topic, message).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        logger.info("------------------------ 消息发送End -------------------");
    }

    // 异步+回调
    public void sendByFuture(String message) {
        logger.info("------------------------ 消息发送Start -------------------");
        logger.info("sendByFuture： Message:" + message + "   Topic:" + topic);

        kafkaTemplate.send(topic, message)
                .addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
                    @Override
                    public void onFailure(Throwable throwable) {
                        logger.error("onFailure: " + throwable.getMessage());
                    }

                    @Override
                    public void onSuccess(SendResult<String, Object> stringObjectSendResult) {
                        logger.info("onSuccess: " + stringObjectSendResult.toString());
                    }
                });

        logger.info("------------------------ 消息发送End -------------------");
    }

    // 使用自定义分区，生产消息到指定区域
    public void sendToMyPartitioner(String message) {
        logger.info("------------------------ 消息发送Start -------------------");
        logger.info("sendToMyPartitioner： Message:" + message + "   Topic:" + topic);

        logger.info("simpleKafkaTemplate: " + simpleKafkaTemplate);
        simpleKafkaTemplate.send(topic, message);

        logger.info("------------------------ 消息发送End -------------------");
    }

    // 使用ack机制，leader持久化即可
    public void sendToAck(String message) {
        logger.info("------------------------ 消息发送Start -------------------");
        logger.info("sendToAck： Message:" + message + "   Topic:" + topic);

        logger.info("ackKafkaTemplate: " + ackKafkaTemplate);
        ackKafkaTemplate.send(topic, message);

        logger.info("------------------------ 消息发送End -------------------");
    }

}
