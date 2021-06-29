package dev.deepesh.springkafkasample;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class TestApplication {

    @Configuration
    public static class KafkaProducerConfig {

        @Autowired
        EmbeddedKafkaBroker broker;

        @Bean
        public ProducerFactory<String, String> producerFactory() {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    this.broker.getBrokersAsString());
            configProps.put(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class);
            configProps.put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class);
            return new DefaultKafkaProducerFactory<>(configProps);
        }

        @Bean
        public KafkaTemplate<String, String> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }
    }

    @EnableKafka
    @Configuration
    static class KafkaConsumerConfig {

        @Autowired
        EmbeddedKafkaBroker broker;

        @Bean
        public KafkaAdmin kafkaAdmin() {
            Map<String, Object> configs = new HashMap<>();
            configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker.getBrokersAsString());
            return new KafkaAdmin(configs);
        }

        @Bean
        public ConsumerFactory<String, String> consumerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    this.broker.getBrokersAsString());
            props.put(
                    ConsumerConfig.GROUP_ID_CONFIG,
                    "groupId");
            props.put(
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
            props.put(
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
            props.put(
                    ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            return new DefaultKafkaConsumerFactory<>(props);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, String> retryTopicListenerContainerFactory(
                ConsumerFactory<String, String> consumerFactory) {

            ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            ContainerProperties props = factory.getContainerProperties();
            props.setIdleEventInterval(100L);
            props.setPollTimeout(50L);
            props.setIdlePartitionEventInterval(100L);
            factory.setConsumerFactory(consumerFactory);
            factory.setConcurrency(1);
            factory.setContainerCustomizer(
                    container -> container.getContainerProperties().setIdlePartitionEventInterval(100L));
            return factory;
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
                ConsumerFactory<String, String> consumerFactory) {

            ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.setConcurrency(1);
            return factory;
        }
    }
}
