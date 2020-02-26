package com.kafka.Service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * @创建人 sgwang
 * @name Receiver
 * @user 91119
 * @创建时间 2020/2/11
 * @描述
 */
@Service
public class Receiver {
    private static final Logger logger = LoggerFactory.getLogger(Sender.class);

    @KafkaListener(topics = "${app.topic}")
    public void listen(ConsumerRecord<?, ?> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());

        if (kafkaMessage.isPresent()) {
            Object message = kafkaMessage.get();
            logger.info("++++++++++++++++++++++ 消息接收Start ++++++++++++++++++++++");

            logger.info("Receive： Record: " + record);
            logger.info("Receive： Message: " + message + "   Topic: " + topic);

            logger.info("++++++++++++++++++++++ 消息接收End ++++++++++++++++++++++");
        }
    }

}
