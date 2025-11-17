package com.sohan.notification_service.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class EnrichedUserConsumer {

    @KafkaListener(
            topics = "${kafka.topics.user-enriched}",
            groupId = "enriched-user-consumer-group"
    )
    public void consumeEnrichedUser(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("========================================");
        log.info("Consumed Enriched User:");
        log.info("Key: {}", key);
        log.info("Value: {}", message);
        log.info("Partition: {}, Offset: {}", partition, offset);
        log.info("========================================");
    }
}
