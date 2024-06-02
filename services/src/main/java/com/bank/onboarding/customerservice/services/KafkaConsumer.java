package com.bank.onboarding.customerservice.services;

import com.bank.onboarding.commonslib.persistence.enums.OperationType;
import com.bank.onboarding.commonslib.utils.kafka.EventSeDeserializer;
import com.bank.onboarding.commonslib.utils.kafka.models.CardAndNetbancoEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.CreateAccountEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.ErrorEvent;
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
        String eventValue = event.value().toString();
        String eventKey = event.key().toString();
        switch (eventKey) {
            case "CREATE_ACCOUNT" -> {
                CreateAccountEvent createAccountEvent = (CreateAccountEvent) eventSeDeserializer.deserialize(eventValue, CreateAccountEvent.class);
                log.info("Event received for account number {}", Optional.ofNullable(createAccountEvent.getAccountRefDTO()).map(AccountRefDTO::getAccountNumber).orElse(""));
                customerService.createCustomerForCreateAccountOperation(createAccountEvent);
            }
            case "CARD_ACCOUNT", "NETBANCO_ACCOUNT" -> {
                CardAndNetbancoEvent cardAndNetbancoEvent = (CardAndNetbancoEvent) eventSeDeserializer.deserialize(eventValue, CardAndNetbancoEvent.class);
                log.info("Event received for customer number {}", Optional.ofNullable(cardAndNetbancoEvent.getCustomerRefDTO()).map(CustomerRefDTO::getCustomerNumber).orElse(""));
                if (OperationType.CARD_ACCOUNT.name().equals(eventKey)) customerService.updateCardCustomer(cardAndNetbancoEvent);
                if (OperationType.NETBANCO_ACCOUNT.name().equals(eventKey)) customerService.updateNetbancoCustomer(cardAndNetbancoEvent);
            }
            default -> {
                ErrorEvent errorEvent = (ErrorEvent) eventSeDeserializer.deserialize(eventValue, ErrorEvent.class);
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
