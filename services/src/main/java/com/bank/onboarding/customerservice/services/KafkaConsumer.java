package com.bank.onboarding.customerservice.services;

import com.bank.onboarding.commonslib.utils.kafka.CreateAccountEvent;
import com.bank.onboarding.commonslib.utils.kafka.ErrorEvent;
import com.bank.onboarding.commonslib.utils.kafka.EventSeDeserializer;
import com.bank.onboarding.commonslib.web.dtos.account.AccountRefDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CustomerRefDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Service;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumer {

    private final EventSeDeserializer eventSeDeserializer;
    private final CustomerService customerService;

    @KafkaListener(topics = "${spring.kafka.consumer.topic-name}",  groupId = "${spring.kafka.consumer.group-id}")
    public void consumeEvent(ConsumerRecord event){
        switch (event.key().toString()) {
            case "CREATE_ACCOUNT" -> {
                CreateAccountEvent createAccountEvent = (CreateAccountEvent) eventSeDeserializer.deserialize(event.value().toString(), CreateAccountEvent.class);
                log.info("Event received for account number {}", Optional.ofNullable(createAccountEvent.getAccountRefDTO()).map(AccountRefDTO::getAccountNumber).orElse(""));
                customerService.createCustomerForCreateAccountOperation(createAccountEvent);
            }
            default -> {
                ErrorEvent errorEvent = (ErrorEvent) eventSeDeserializer.deserialize(event.value().toString(), ErrorEvent.class);
                log.info("Error event {} received for customer number {}", errorEvent, Optional.ofNullable(errorEvent.getCustomerRefDTO()).map(CustomerRefDTO::getCustomerNumber).orElse(""));
                customerService.handleErrorEvent(errorEvent);
            }
        }
    }

    @Bean
    public DefaultErrorHandler errorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(0, 0));
    }
}
