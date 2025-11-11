package com.sohan.notification_service.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DeadLetterConsumer {
    private static final Logger log = LoggerFactory.getLogger(DeadLetterConsumer.class);

    @KafkaListener(topics = "user-events-dlq", groupId = "notification-group-dlq")
    public void consume(String message) {
        log.warn("Received message in DLQ: {}", message);
    }
}
