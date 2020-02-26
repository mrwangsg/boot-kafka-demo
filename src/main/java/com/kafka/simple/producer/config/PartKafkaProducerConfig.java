package com.kafka.simple.producer.config;

import com.kafka.simple.producer.partition.MyPartitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @创建人 sgwang
 * @name PartKafkaProducerConfig
 * @user 91119
 * @创建时间 2020/2/11
 * @描述
 */
@EnableKafka
@Component("partKafkaProducerConfig")
public class PartKafkaProducerConfig {

    @Value("${kafka.producer.servers}")
    private String servers;

    public ProducerFactory<String, Object> producerFactory() {
        // 必须配置的三个参数，否则spring会注入默认值，不合适远端调用的值
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 使用自定义 生成者分区策略
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class);

        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean(name = "part_kafka_template")
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
