package com.sohan.notification_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
public class KafkaConfig {

//    @Bean
//    public NewTopic userEventsTopic() {
//        return new NewTopic("user-events", 1, (short) 1);
//    }
//
//    @Bean
//    public NewTopic userEventsDlqTopic() {
//        return new NewTopic("user-events-dlq", 1, (short) 1);
//    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        //publish to dlq after 3 retries
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template,
                ((consumerRecord, e) -> new TopicPartition("user-events-dlq", consumerRecord.partition()))
        );
        return new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            org.springframework.kafka.core.ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler errorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
