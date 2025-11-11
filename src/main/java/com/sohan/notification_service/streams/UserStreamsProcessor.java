package com.sohan.notification_service.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class UserStreamsProcessor {

    @Bean
    public KStream<String, String> processUserEvents(StreamsBuilder builder) {
        KStream<String, String> stream = builder.stream(
                "user-events",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // transform
        KStream<String, String> processed = stream.mapValues(value -> value.toUpperCase());

        processed.to("processed-users", Produced.with(Serdes.String(), Serdes.String()));

        return stream;
    }
}
