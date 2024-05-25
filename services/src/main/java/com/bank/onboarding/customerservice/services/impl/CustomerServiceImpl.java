package com.bank.onboarding.customerservice.services.impl;

import com.bank.onboarding.commonslib.persistence.exceptions.OnboardingException;
import com.bank.onboarding.commonslib.persistence.models.Contact;
import com.bank.onboarding.commonslib.persistence.models.Customer;
import com.bank.onboarding.commonslib.persistence.models.identifiers.AccountIdentifier;
import com.bank.onboarding.commonslib.persistence.models.identifiers.ContactIdentifier;
import com.bank.onboarding.commonslib.persistence.services.AccountRefRepoService;
import com.bank.onboarding.commonslib.persistence.services.ContactRepoService;
import com.bank.onboarding.commonslib.persistence.services.CustomerRepoService;
import com.bank.onboarding.commonslib.utils.kafka.CreateAccountEvent;
import com.bank.onboarding.commonslib.utils.kafka.KafkaProducer;
import com.bank.onboarding.commonslib.utils.mappers.AccountMapper;
import com.bank.onboarding.commonslib.web.dtos.account.CreateAccountRequestDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.ContactDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CustomerRefDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.DocumentIdDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.TaxIdDTO;
import com.bank.onboarding.customerservice.services.CustomerService;
import io.micrometer.common.util.StringUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.bank.onboarding.commonslib.persistence.constants.OnboardingConstants.CONTACT_TYPES;
import static com.bank.onboarding.commonslib.persistence.constants.OnboardingConstants.CUSTOMER_TYPES;
import static com.bank.onboarding.commonslib.persistence.constants.OnboardingConstants.DOCUMENT_TYPES_CREATE_ACCOUNT_REQUEST;
import static com.bank.onboarding.commonslib.persistence.constants.OnboardingConstants.faker;
import static com.bank.onboarding.commonslib.persistence.enums.ContactType.EMAIL;
import static com.bank.onboarding.commonslib.persistence.enums.ContactType.TELEPHONE;

@Slf4j
@Service
@RequiredArgsConstructor
public class CustomerServiceImpl implements CustomerService {

    private final CustomerRepoService customerRepoService;
    private final ContactRepoService contactRepoService;
    private final AccountRefRepoService accountRefRepoService;
    private final KafkaProducer kafkaProducer;

    @Override
    public void createCustomerForCreateAccountOperation(CreateAccountEvent createAccountEvent) {
        CreateAccountRequestDTO createAccountRequestDTO = createAccountEvent.getCreateAccountRequestDTO();
        validateCustomer(createAccountRequestDTO);

        Contact contact = contactRepoService.saveContactDB(Contact.builder()
                .type(createAccountRequestDTO.getCustomerContact().getType())
                .value(createAccountRequestDTO.getCustomerContact().getValue())
                .creationTime(LocalDateTime.now())
                .lastUpdateTime(LocalDateTime.now())
                .build());

        Customer customer = customerRepoService.saveCustomerDB(Customer.builder()
                .accounts(List.of(AccountIdentifier.builder().accountId(createAccountEvent.getAccountRefDTO().getAccountId()).build()))
                .birthDate(createAccountRequestDTO.getCustomerBirthDate())
                .contacts(List.of(ContactIdentifier.builder().contactId(contact.getId()).build()))
                .creationTime(LocalDateTime.now())
                .documentIdCountry(createAccountRequestDTO.getCustomerDocId().getDocumentIdCountry())
                .documentIdNumber(createAccountRequestDTO.getCustomerDocId().getDocumentIdNumber())
                .documentIdType(createAccountRequestDTO.getCustomerDocId().getDocumentIdType())
                .documentIdExpirationDate(createAccountRequestDTO.getCustomerDocId().getDocumentIdExpirationDate())
                .firstName(createAccountRequestDTO.getCustomerFirstName())
                .intervenientIndicator(Boolean.TRUE)
                .lastName(createAccountRequestDTO.getCustomerLastName())
                .lastUpdateTime(LocalDateTime.now())
                .nationality("Português")
                .number(String.valueOf('C' + ((int) faker.number().randomNumber(9, true))))
                .taxIdCountry(createAccountRequestDTO.getCustomerTaxId().getTaxIdCountry())
                .taxIdNumber(createAccountRequestDTO.getCustomerTaxId().getTaxIdNumber())
                .taxIdType(createAccountRequestDTO.getCustomerTaxId().getTaxIdType())
                .type(createAccountRequestDTO.getCustomerType())
                .build());

        accountRefRepoService.saveAccountRefDB(AccountMapper.INSTANCE.toAccountRef(createAccountEvent.getAccountRefDTO()));
        createAccountEvent.setCustomerRefDTO(CustomerRefDTO.builder()
                .customerId(customer.getId())
                .customerNumber(customer.getNumber())
                .build());

        kafkaProducer.sendEvent("${spring.kafka.producer.intervention.topic-name}", createAccountEvent);
        kafkaProducer.sendEvent("${spring.kafka.producer.document.topic-name}", createAccountEvent);
    }

    private void validateCustomer(CreateAccountRequestDTO createAccountRequestDTO) {
        if(Boolean.FALSE.equals(validateContact(createAccountRequestDTO.getCustomerContact())) ||
                Boolean.FALSE.equals(validateDocId(createAccountRequestDTO.getCustomerDocId())) ||
                Boolean.FALSE.equals( validateTaxId(createAccountRequestDTO.getCustomerTaxId())) ||
                StringUtils.isBlank(createAccountRequestDTO.getCustomerBirthDate().toLocalDate().toString()) ||
                StringUtils.isBlank(createAccountRequestDTO.getCustomerFirstName()) ||
                StringUtils.isBlank(createAccountRequestDTO.getCustomerLastName())){
            //TODO -> Enviar evento para eliminar conta
            throw new OnboardingException("Houve um problema com o seu pedido, por favor verifique as suas informações enviadas!");
        } else if (!CUSTOMER_TYPES.contains(Optional.ofNullable(createAccountRequestDTO.getCustomerType()).orElse(""))) {
            //TODO -> Enviar evento para eliminar conta
            throw new OnboardingException("O tipo de cliente inserido não existe, tente novamente!");
        }
    }

    private boolean validateContact(ContactDTO customerContact) {
        String customerContactType = Optional.ofNullable(customerContact).map(ContactDTO::getType).orElse("");
        String customerContactValue = Optional.ofNullable(customerContact).map(ContactDTO::getValue).orElse("");

        if(!CONTACT_TYPES.contains(customerContactType) || (TELEPHONE.name().equals(customerContactType) &&
                !Pattern.compile("^(\\d{3}[ ]?){2}\\d{3}$").matcher(customerContactValue).matches()) ||
                (EMAIL.name().equals(customerContactType) && !Pattern.compile("^(.+)@(\\S+) $").matcher(customerContactValue).matches())){
            //TODO -> Enviar evento para eliminar conta
            throw new OnboardingException("O tipo de contacto inserido não é válido!");
        }

        return true;
    }
    private boolean validateDocId(DocumentIdDTO customerDocId) {
        String customerDocIdNumber = Optional.ofNullable(customerDocId).map(DocumentIdDTO::getDocumentIdNumber).orElse("");
        LocalDateTime actualTime = LocalDateTime.now();
        LocalDateTime customerDocIdExpirationdate = Optional.ofNullable(customerDocId).map(DocumentIdDTO::getDocumentIdExpirationDate).orElse(actualTime);

        if (!DOCUMENT_TYPES_CREATE_ACCOUNT_REQUEST.contains(Optional.ofNullable(customerDocId).map(DocumentIdDTO::getDocumentIdType).orElse("")) ||
                !Pattern.compile("^\\d{9}$").matcher(customerDocIdNumber).matches() ||
                Boolean.FALSE.equals(customerDocIdExpirationdate.toLocalDate().isAfter(actualTime.toLocalDate()))){
            //TODO -> Enviar evento para eliminar conta
            throw new OnboardingException("O tipo de documento inserido não é valido!");
        }

        return true;
    }
    private boolean validateTaxId(TaxIdDTO customerTaxId) {
        String customerDocIdNumber = Optional.ofNullable(customerTaxId).map(TaxIdDTO::getTaxIdNumber).orElse("");

        if (!DOCUMENT_TYPES_CREATE_ACCOUNT_REQUEST.contains(Optional.ofNullable(customerTaxId).map(TaxIdDTO::getTaxIdType).orElse("")) ||
                !Pattern.compile("^\\d{10}$").matcher(customerDocIdNumber).matches()){
            //TODO -> Enviar evento para eliminar conta
            throw new OnboardingException("O tipo de documento inserido não é valido!");
        }

        return true;
    }
}
