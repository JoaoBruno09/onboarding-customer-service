package com.bank.onboarding.customerservice.services;

import com.bank.onboarding.commonslib.utils.kafka.CreateAccountEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${spring.kafka.consumer.topic-name}",  groupId = "${spring.kafka.consumer.group-id}")
    public void consumeEvent(ConsumerRecord event){
        try {
            objectMapper.registerModule(new JavaTimeModule());
            String eventType = objectMapper.readTree(event.value().toString()).asText();
            CreateAccountEvent createAccountEvent = objectMapper.readValue(eventType, CreateAccountEvent.class);
            log.info("Event received is " + createAccountEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
