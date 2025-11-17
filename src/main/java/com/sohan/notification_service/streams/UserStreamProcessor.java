package com.sohan.notification_service.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sohan.notification_service.model.EnrichedUser;
import com.sohan.notification_service.model.UserEvent;
import com.sohan.notification_service.model.UserLocation;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class UserStreamProcessor {

    @Value("${kafka.topics.user-events}")
    private String userEventsTopic;

    @Value("${kafka.topics.user-location}")
    private String userLocationTopic;

    @Value("${kafka.topics.user-enriched}")
    private String userEnrichedTopic;

    private final ObjectMapper objectMapper = new ObjectMapper();

//    @Bean
//    public KStream<String, String> processUserEvents(StreamsBuilder builder) {
//        KStream<String, String> stream = builder.stream(
//                "user-events",
//                Consumed.with(Serdes.String(), Serdes.String())
//        );
//
//        // transform
//        KStream<String, String> processed = stream.mapValues(value -> value.toUpperCase());
//
//        processed.to("processed-users", Produced.with(Serdes.String(), Serdes.String()));
//
//        return stream;
//    }

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        // Create KStream for user-events
        KStream<String, String> userEventsStream = streamsBuilder
                .stream(userEventsTopic, Consumed.with(Serdes.String(), Serdes.String()));

        // Create KTable for user-location (for lookup)
        KTable<String, String> userLocationTable = streamsBuilder
                .table(userLocationTopic, Consumed.with(Serdes.String(), Serdes.String()));

        // Join the stream with the table
        KStream<String, String> enrichedStream = userEventsStream
                .leftJoin(userLocationTable,
                        (userEventJson, userLocationJson) -> {
                            try {
                                log.info("Joining - UserEvent: {}, UserLocation: {}", userEventJson, userLocationJson);

                                UserEvent userEvent = objectMapper.readValue(userEventJson, UserEvent.class);

                                EnrichedUser enrichedUser = new EnrichedUser();
                                enrichedUser.setId(userEvent.getId());
                                enrichedUser.setName(userEvent.getName());
                                enrichedUser.setEmail(userEvent.getEmail());

                                if (userLocationJson != null) {
                                    UserLocation userLocation = objectMapper.readValue(userLocationJson, UserLocation.class);
                                    enrichedUser.setCity(userLocation.getCity());
                                } else {
                                    enrichedUser.setCity("Unknown");
                                }

                                String result = objectMapper.writeValueAsString(enrichedUser);
                                log.info("Enriched User: {}", result);
                                return result;
                            } catch (Exception e) {
                                log.error("Error during join", e);
                                return null;
                            }
                        });

        // Write to output topic
        enrichedStream
                .filter((key, value) -> value != null)
                .to(userEnrichedTopic, Produced.with(Serdes.String(), Serdes.String()));

        log.info("Kafka Streams topology built successfully");
    }
}
