package com.bank.onboarding.customerservice.services.impl;

import com.bank.onboarding.commonslib.persistence.enums.OperationType;
import com.bank.onboarding.commonslib.persistence.exceptions.OnboardingException;
import com.bank.onboarding.commonslib.persistence.models.Contact;
import com.bank.onboarding.commonslib.persistence.models.Customer;
import com.bank.onboarding.commonslib.persistence.models.identifiers.AccountIdentifier;
import com.bank.onboarding.commonslib.persistence.models.identifiers.ContactIdentifier;
import com.bank.onboarding.commonslib.persistence.services.AccountRefRepoService;
import com.bank.onboarding.commonslib.persistence.services.ContactRepoService;
import com.bank.onboarding.commonslib.persistence.services.CustomerRepoService;
import com.bank.onboarding.commonslib.utils.OnboardingUtils;
import com.bank.onboarding.commonslib.utils.kafka.CreateAccountEvent;
import com.bank.onboarding.commonslib.utils.kafka.ErrorEvent;
import com.bank.onboarding.commonslib.utils.kafka.KafkaProducer;
import com.bank.onboarding.commonslib.utils.mappers.AccountMapper;
import com.bank.onboarding.commonslib.web.dtos.account.AccountRefDTO;
import com.bank.onboarding.commonslib.web.dtos.account.CreateAccountRequestDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.ContactDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CustomerRefDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.DocumentIdDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.TaxIdDTO;
import com.bank.onboarding.customerservice.services.CustomerService;
import io.micrometer.common.util.StringUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
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
import static com.bank.onboarding.commonslib.persistence.enums.OperationType.CREATE_ACCOUNT;
import static com.bank.onboarding.commonslib.persistence.enums.OperationType.UPDATE_CUSTOMER_REF;

@Slf4j
@Service
@RequiredArgsConstructor
public class CustomerServiceImpl implements CustomerService {

    private final CustomerRepoService customerRepoService;
    private final ContactRepoService contactRepoService;
    private final AccountRefRepoService accountRefRepoService;
    private final KafkaProducer kafkaProducer;
    private final OnboardingUtils onboardingUtils;

    @Value("${spring.kafka.producer.intervention.topic-name}")
    private String interventionTopicName;

    @Value("${spring.kafka.producer.document.topic-name}")
    private String documentTopicName;

    @Value("${spring.kafka.producer.account.topic-name}")
    private String accountTopicName;

    @Value("${spring.kafka.producer.relation.topic-name}")
    private String relationTopicName;

    @Override
    public void createCustomerForCreateAccountOperation(CreateAccountEvent createAccountEvent) {
        CreateAccountRequestDTO createAccountRequestDTO = createAccountEvent.getCreateAccountRequestDTO();
        validateCustomer(createAccountRequestDTO,createAccountEvent.getAccountRefDTO(), CREATE_ACCOUNT);

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
                .number("C" + ((int) faker.number().randomNumber(9, true)))
                .taxIdCountry(createAccountRequestDTO.getCustomerTaxId().getTaxIdCountry())
                .taxIdNumber(createAccountRequestDTO.getCustomerTaxId().getTaxIdNumber())
                .taxIdType(createAccountRequestDTO.getCustomerTaxId().getTaxIdType())
                .type(createAccountRequestDTO.getCustomerType())
                .build());

        accountRefRepoService.saveAccountRefDB(AccountMapper.INSTANCE.toAccountRef(createAccountEvent.getAccountRefDTO()));
        CustomerRefDTO customerRefDTO = CustomerRefDTO.builder().customerId(customer.getId()).customerNumber(customer.getNumber()).build();
        createAccountEvent.setCustomerRefDTO(customerRefDTO);

        kafkaProducer.sendEvent(interventionTopicName, CREATE_ACCOUNT, createAccountEvent);
        kafkaProducer.sendEvent(documentTopicName, CREATE_ACCOUNT, createAccountEvent);
        kafkaProducer.sendEvent(accountTopicName, UPDATE_CUSTOMER_REF , customerRefDTO);
        kafkaProducer.sendEvent(relationTopicName, UPDATE_CUSTOMER_REF , customerRefDTO);
    }

    @Override
    public void handleErrorEvent(ErrorEvent errorEvent) {
        if(CREATE_ACCOUNT.equals(errorEvent.getOperationType())){
            Customer customerToDeleteAccount = customerRepoService.getCustomerById(errorEvent.getCustomerRefDTO().getCustomerId());
            String accountId = errorEvent.getAccountRefDTO().getAccountId();
            customerToDeleteAccount.setAccounts(customerToDeleteAccount.getAccounts().stream().filter(accountIdentifier -> !accountId.equals(accountIdentifier.getAccountId())).toList());
            customerRepoService.saveCustomerDB(customerToDeleteAccount);
            accountRefRepoService.deleteAccountById(errorEvent.getAccountRefDTO().getAccountId());
        }
    }

    private void validateCustomer(CreateAccountRequestDTO createAccountRequestDTO, AccountRefDTO accountRefDTO, OperationType operationType) {
        validateContact(createAccountRequestDTO.getCustomerContact(), accountRefDTO, operationType);
        validateDocId(createAccountRequestDTO.getCustomerDocId(), accountRefDTO, operationType);
        validateTaxId(createAccountRequestDTO.getCustomerTaxId(), accountRefDTO, operationType);
        if(StringUtils.isBlank(createAccountRequestDTO.getCustomerBirthDate().toLocalDate().toString()) ||
                StringUtils.isBlank(createAccountRequestDTO.getCustomerFirstName()) ||
                StringUtils.isBlank(createAccountRequestDTO.getCustomerLastName())){

            onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);
            throw new OnboardingException("Houve um problema com o seu pedido, por favor verifique as suas informações enviadas!");
        } else if (!CUSTOMER_TYPES.contains(Optional.ofNullable(createAccountRequestDTO.getCustomerType()).orElse(""))) {
            onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);
            throw new OnboardingException("O tipo de cliente inserido não existe, tente novamente!");
        }
    }

    private void validateContact(ContactDTO customerContact, AccountRefDTO accountRefDTO, OperationType operationType) {
        String customerContactType = Optional.ofNullable(customerContact).map(ContactDTO::getType).orElse("");
        String customerContactValue = Optional.ofNullable(customerContact).map(ContactDTO::getValue).orElse("");

        if(!CONTACT_TYPES.contains(customerContactType) || (TELEPHONE.name().equals(customerContactType) &&
                !Pattern.compile("^(\\d{3}[ ]?){2}\\d{3}$").matcher(customerContactValue).matches()) ||
                (EMAIL.name().equals(customerContactType) && !Pattern.compile("^(.+)@(\\S+) $").matcher(customerContactValue).matches())){
            onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);
            throw new OnboardingException("O tipo de contacto inserido não é válido!");
        }
    }
    private void validateDocId(DocumentIdDTO customerDocId, AccountRefDTO accountRefDTO, OperationType operationType) {
        String customerDocIdNumber = Optional.ofNullable(customerDocId).map(DocumentIdDTO::getDocumentIdNumber).orElse("").trim();
        LocalDateTime actualTime = LocalDateTime.now();
        LocalDateTime customerDocIdExpirationdate = Optional.ofNullable(customerDocId).map(DocumentIdDTO::getDocumentIdExpirationDate).orElse(actualTime);

        if (!DOCUMENT_TYPES_CREATE_ACCOUNT_REQUEST.contains(Optional.ofNullable(customerDocId).map(DocumentIdDTO::getDocumentIdType).orElse("")) ||
                !Pattern.compile("^\\d{8} \\d [A-Z]{2}\\d$").matcher(customerDocIdNumber).matches() ||
                Boolean.FALSE.equals(customerDocIdExpirationdate.toLocalDate().isAfter(actualTime.toLocalDate()))){
            onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);
            throw new OnboardingException("Ocorreu um erro. Verifique os campos do documento de identificação!");
        }
    }
    private void validateTaxId(TaxIdDTO customerTaxId, AccountRefDTO accountRefDTO, OperationType operationType) {
        String customerDocIdNumber = Optional.ofNullable(customerTaxId).map(TaxIdDTO::getTaxIdNumber).orElse("").trim();

        if (!DOCUMENT_TYPES_CREATE_ACCOUNT_REQUEST.contains(Optional.ofNullable(customerTaxId).map(TaxIdDTO::getTaxIdType).orElse("")) ||
                !Pattern.compile("^\\d{9}$").matcher(customerDocIdNumber).matches()){
            onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);
            throw new OnboardingException("O tipo de documento inserido não é valido!");
        }
    }
}
