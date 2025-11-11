package com.sohan.notification_service.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
@Component
public class UserEventConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(UserEventConsumer.class);

//    @KafkaListener (topics = "user-events", groupId = "notification-group", containerFactory = "kafkaListenerContainerFactory")
//    private void listen(String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition, @Header(KafkaHeaders.OFFSET) long offset) {
//        try {
//            System.out.println("\nReceived event with: partition " + partition + " | offset " + offset);
//            System.out.println(objectMapper.readTree(message).toPrettyString());
//        } catch (Exception e) {
//            System.out.println("Failed to parse message: " + message);
//        }
//    }

    @KafkaListener(topics = "user-events", groupId = "notification-group")
    public void consume(ConsumerRecord<String, String> record) {
        log.info("Consumer message: {}", record.value());

        //simulate failure
        if (record.value().contains("error")) {
            throw new RuntimeException("Failed to process event: " + record.value());
        }

        log.info("Successfully processed: {}", record.value());
    }
}
