package com.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
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

    @KafkaListener(id = "part0", groupId = "${kafka.consumer.group.id}", containerFactory = "defaultContainerFactory",
            topicPartitions = {@TopicPartition(topic = "${app.topic}", partitions = {"0"})})
    public void listen0(ConsumerRecord<?, ?> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        logger.info("Thread: name: " + Thread.currentThread().getName() + "   listen0: " + Thread.currentThread().toString());
        printRecord(record, topic);
    }

    @KafkaListener(id = "part1", groupId = "${kafka.consumer.group.id}", containerFactory = "defaultContainerFactory",
            topicPartitions = {@TopicPartition(topic = "${app.topic}", partitions = {"1"})})
    public void listen1(ConsumerRecord<?, ?> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        logger.info("Thread: name: " + Thread.currentThread().getName() + "   listen1: " + Thread.currentThread().toString());
        printRecord(record, topic);
    }

    @KafkaListener(id = "offset", groupId = "${kafka.consumer.group.id.offset}", containerFactory = "offsetContainerFactory",
            topics = "${app.topic}")
    public void offsetListen(ConsumerRecord<?, ?> record, Acknowledgment ack, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        logger.info("Thread: name: " + Thread.currentThread().getName() + "   offsetListen: " + Thread.currentThread().toString());

        ack.acknowledge(); // 主动提交offset. 不调用的话，会出现zk该"消费组"的协同者，会一直没有更新offset值，最直接的现象是，每次重启应用，会重新消费offset

        printRecord(record, topic);
    }

    private void printRecord(ConsumerRecord<?, ?> record, String topic) {
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
