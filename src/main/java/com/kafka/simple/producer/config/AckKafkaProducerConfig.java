package com.kafka.simple.producer.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @创建人 sgwang
 * @name AckKafkaProducerConfig
 * @user 91119
 * @创建时间 2020/2/26
 * @描述
 */

@EnableKafka
@Component("ackKafkaProducerConfig")
public class AckKafkaProducerConfig {

    @Value("${kafka.producer.servers}")
    private String servers;

    public ProducerFactory<String, Object> producerFactory() {
        // 必须配置的三个参数，否则spring会注入默认值，不合适远端调用的值
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // ack保证机制，有0(发送即可)、1(leader本地持久化)、-1或all(所有in-sync replicas持久化)【不推荐.  leader挂掉，有风险】
        configs.put(ProducerConfig.ACKS_CONFIG, "1");

        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean(name = "ack_kafka_template")
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}