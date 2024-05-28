package com.bank.onboarding.customerservice.services;

import com.bank.onboarding.commonslib.utils.kafka.CreateAccountEvent;
import com.bank.onboarding.commonslib.utils.kafka.EventSeDeserializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumer {

    private final EventSeDeserializer eventSeDeserializer;

    @KafkaListener(topics = "${spring.kafka.consumer.topic-name}",  groupId = "${spring.kafka.consumer.group-id}")
    public void consumeEvent(ConsumerRecord event) throws JsonProcessingException {
        switch (event.key().toString()){
            case "CREATE_ACCOUNT":
                CreateAccountEvent createAccountEvent = (CreateAccountEvent) eventSeDeserializer.deserialize(event.value().toString(), CreateAccountEvent.class);
                log.info("Event received is {}", createAccountEvent);
        }
    }

}
