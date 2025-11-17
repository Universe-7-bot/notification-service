package com.sohan.notification_service.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JoinTopology {

    private final ObjectMapper objectMapper = new ObjectMapper();

//    @Bean
//    public KStream<String, String> kStream(StreamsBuilder builder) {
//        //'user-events'
//        KStream<String, String> userEvents = builder.stream(
//                "user-events",
//                Consumed.with(Serdes.String(), Serdes.String())
//        );
//
//        //user-location
//        KTable<String, String> userLocation = builder.table(
//                "user-location",
//                Consumed.with(Serdes.String(), Serdes.String())
//        );
//
//        //join
//        KStream<String, String> enriched = userEvents.join(
//                userLocation,
//                (event, location) -> {
//                    try {
//                        return objectMapper.createObjectNode()
//                                .put("event", event)
//                                .put("location", location == null ? "null" : location)
//                                .toString();
//                    } catch (Exception e) {
//                        return "{\"error\":\"join failed\"}";
//                    }
//                }
//        ).peek((k, v) ->
//                System.out.println("ENRICHED → key=" + k + " value=" + v)
//        );;
//
//        //publishing
//        enriched.to("user-enriched", Produced.with(Serdes.String(), Serdes.String()));
//
//        return enriched;
//    }

//    @Bean
//    public KStream<String, String> kStream(StreamsBuilder builder) {
//
//        KStream<String, String> userEvents =
//                builder.stream("user-events", Consumed.with(Serdes.String(), Serdes.String()));
//
//        KTable<String, String> userLocation =
//                builder.table("user-location",
//                        Consumed.with(Serdes.String(), Serdes.String()),
//                        Materialized.as("user-location-store"));
//
//        KStream<String, String> enriched = userEvents.join(
//                userLocation,
//                (event, location) -> {
//                    try {
//                        return objectMapper.createObjectNode()
//                                .put("event", event)
//                                .put("location", location == null ? "null" : location)
//                                .toString();
//                    } catch (Exception ex) {
//                        return "{\"error\":\"join failed\"}";
//                    }
//                }
//        ).peek((k, v) ->
//                System.out.println("ENRICHED → key=" + k + " value=" + v)
//        );
//
//        enriched.to("user-enriched", Produced.with(Serdes.String(), Serdes.String()));
//
//        return enriched;
//    }
}
