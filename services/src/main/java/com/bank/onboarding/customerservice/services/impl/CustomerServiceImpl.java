package com.bank.onboarding.customerservice.services.impl;

import com.bank.onboarding.commonslib.persistence.enums.OperationType;
import com.bank.onboarding.commonslib.persistence.exceptions.OnboardingException;
import com.bank.onboarding.commonslib.persistence.models.AccountRef;
import com.bank.onboarding.commonslib.persistence.models.Address;
import com.bank.onboarding.commonslib.persistence.models.Contact;
import com.bank.onboarding.commonslib.persistence.models.Customer;
import com.bank.onboarding.commonslib.persistence.models.identifiers.AccountIdentifier;
import com.bank.onboarding.commonslib.persistence.models.identifiers.AddressIdentifier;
import com.bank.onboarding.commonslib.persistence.models.identifiers.ContactIdentifier;
import com.bank.onboarding.commonslib.persistence.services.AccountRefRepoService;
import com.bank.onboarding.commonslib.persistence.services.AddressRepoService;
import com.bank.onboarding.commonslib.persistence.services.ContactRepoService;
import com.bank.onboarding.commonslib.persistence.services.CustomerRepoService;
import com.bank.onboarding.commonslib.utils.OnboardingUtils;
import com.bank.onboarding.commonslib.utils.kafka.KafkaProducer;
import com.bank.onboarding.commonslib.utils.kafka.models.CardAndNetbancoEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.CreateAccountEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.CreateIntervenientEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.CreateRelationEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.ErrorEvent;
import com.bank.onboarding.commonslib.utils.mappers.AccountMapper;
import com.bank.onboarding.commonslib.utils.mappers.CustomerMapper;
import com.bank.onboarding.commonslib.web.dtos.account.AccountRefDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.AddressDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.ContactDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CreateIntervenientDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CreateRelationDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CustomerDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CustomerRefDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CustomerRequestDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.DocumentIdDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.TaxIdDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.UpdateCustomerRequestDTO;
import com.bank.onboarding.customerservice.services.CustomerService;
import io.micrometer.common.util.StringUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.bank.onboarding.commonslib.persistence.constants.OnboardingConstants.CONTACT_TYPES;
import static com.bank.onboarding.commonslib.persistence.constants.OnboardingConstants.CUSTOMER_TYPES;
import static com.bank.onboarding.commonslib.persistence.constants.OnboardingConstants.DOCUMENT_TYPES_CREATE_ACCOUNT_REQUEST;
import static com.bank.onboarding.commonslib.persistence.constants.OnboardingConstants.faker;
import static com.bank.onboarding.commonslib.persistence.enums.ContactType.EMAIL;
import static com.bank.onboarding.commonslib.persistence.enums.ContactType.TELEPHONE;
import static com.bank.onboarding.commonslib.persistence.enums.OperationType.ADD_INTERVENIENT;
import static com.bank.onboarding.commonslib.persistence.enums.OperationType.ADD_REL;
import static com.bank.onboarding.commonslib.persistence.enums.OperationType.CREATE_ACCOUNT;
import static com.bank.onboarding.commonslib.persistence.enums.OperationType.UPDATE_CUSTOMER_REF;

@Slf4j
@Service
@RequiredArgsConstructor
public class CustomerServiceImpl implements CustomerService {

    private final CustomerRepoService customerRepoService;
    private final ContactRepoService contactRepoService;
    private final AccountRefRepoService accountRefRepoService;
    private final AddressRepoService addressRepoService;
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
        CustomerRequestDTO customerRequestDTO = createAccountEvent.getCreateAccountRequestDTO().getCustomerIntervenient();
        validateCustomer(customerRequestDTO,createAccountEvent.getAccountRefDTO(), CREATE_ACCOUNT);
        Customer customer = createNewCustomer(customerRequestDTO, createAccountEvent.getAccountRefDTO().getAccountId());

        accountRefRepoService.saveAccountRefDB(AccountMapper.INSTANCE.toAccountRef(createAccountEvent.getAccountRefDTO()));
        CustomerRefDTO customerRefDTO = CustomerRefDTO.builder().customerId(customer.getId()).customerNumber(customer.getNumber()).build();
        createAccountEvent.setCustomerRefDTO(customerRefDTO);

        kafkaProducer.sendEvent(interventionTopicName, CREATE_ACCOUNT, createAccountEvent);
        kafkaProducer.sendEvent(documentTopicName, CREATE_ACCOUNT, createAccountEvent);
        kafkaProducer.sendEvent(accountTopicName, UPDATE_CUSTOMER_REF , customerRefDTO);
        kafkaProducer.sendEvent(relationTopicName, UPDATE_CUSTOMER_REF , customerRefDTO);
    }

    private Customer createNewCustomer(CustomerRequestDTO customerRequestDTO, String accountId) {
        Contact contact = contactRepoService.saveContactDB(Contact.builder()
                .type(customerRequestDTO.getCustomerContact().getType())
                .value(customerRequestDTO.getCustomerContact().getValue())
                .creationTime(LocalDateTime.now())
                .lastUpdateTime(LocalDateTime.now())
                .build());

       return customerRepoService.saveCustomerDB(Customer.builder()
                .accounts(List.of(AccountIdentifier.builder().accountId(accountId).build()))
                .birthDate(customerRequestDTO.getCustomerBirthDate())
                .contacts(List.of(ContactIdentifier.builder().contactId(contact.getId()).build()))
                .creationTime(LocalDateTime.now())
                .documentIdCountry(customerRequestDTO.getCustomerDocId().getDocumentIdCountry())
                .documentIdNumber(customerRequestDTO.getCustomerDocId().getDocumentIdNumber())
                .documentIdType(customerRequestDTO.getCustomerDocId().getDocumentIdType())
                .documentIdExpirationDate(customerRequestDTO.getCustomerDocId().getDocumentIdExpirationDate())
                .firstName(customerRequestDTO.getCustomerFirstName())
                .intervenientIndicator(Boolean.TRUE)
                .lastName(customerRequestDTO.getCustomerLastName())
                .lastUpdateTime(LocalDateTime.now())
                .nationality("Português")
                .number("C" + ((int) faker.number().randomNumber(9, true)))
                .taxIdCountry(customerRequestDTO.getCustomerTaxId().getTaxIdCountry())
                .taxIdNumber(customerRequestDTO.getCustomerTaxId().getTaxIdNumber())
                .taxIdType(customerRequestDTO.getCustomerTaxId().getTaxIdType())
                .type(customerRequestDTO.getCustomerType())
                .build());
    }

    @Override
    public void handleErrorEvent(ErrorEvent errorEvent) {
        if(CREATE_ACCOUNT.equals(errorEvent.getOperationType())){
            Customer customerToDeleteAccount = customerRepoService.getCustomerById(errorEvent.getCustomerRefDTO().getCustomerId());
            String accountId = errorEvent.getAccountRefDTO().getAccountId();
            customerToDeleteAccount.setAccounts(customerToDeleteAccount.getAccounts().stream().filter(accountIdentifier -> !accountId.equals(accountIdentifier.getAccountId())).toList());
            customerRepoService.saveCustomerDB(customerToDeleteAccount);
            accountRefRepoService.deleteAccountById(errorEvent.getAccountRefDTO().getAccountId());
        } else if (ADD_INTERVENIENT.equals(errorEvent.getOperationType())){
            customerRepoService.deleteCustomerById(errorEvent.getCustomerRefDTO().getCustomerId());
        }
    }

    @Override
    public CustomerDTO updateCustomer(String customerNumber, UpdateCustomerRequestDTO updateCustomerRequestDTO) {
        if(Stream.of(updateCustomerRequestDTO).anyMatch(Objects::isNull))
            throw new OnboardingException("Houve um problema com o seu pedido, por favor verifique as suas informações enviadas!");

        onboardingUtils.isValidPhase(updateCustomerRequestDTO.getAccountPhase(), OperationType.UPDATE_CUSTOMER);

        CustomerDTO customerUpdated = CustomerMapper.INSTANCE.toCustomerDTO(
                customerRepoService.saveCustomerDB(buildUpdatedCustomer(customerRepoService.getCustomerByNumber(customerNumber), updateCustomerRequestDTO)));
        customerUpdated.setAddresses(updateCustomerRequestDTO.getAddresses());
        customerUpdated.setContacts(updateCustomerRequestDTO.getContacts());

        return customerUpdated;
    }

    @Override
    public void updateCardCustomer(CardAndNetbancoEvent cardAndNetbancoEvent) {
        Customer customer = customerRepoService.getCustomerByNumber(cardAndNetbancoEvent.getCustomerRefDTO().getCustomerNumber());
        customer.setCardIndicator(cardAndNetbancoEvent.isValue());
        customerRepoService.saveCustomerDB(customer);
    }

    @Override
    public void updateNetbancoCustomer(CardAndNetbancoEvent cardAndNetbancoEvent) {
        Customer customer = customerRepoService.getCustomerByNumber(cardAndNetbancoEvent.getCustomerRefDTO().getCustomerNumber());
        customer.setOnlineBankingIndicator(cardAndNetbancoEvent.isValue());
        customerRepoService.saveCustomerDB(customer);
    }

    @Override
    public CustomerDTO createIntervenientOrAddIntervention(CreateIntervenientDTO createIntervenientDTO) {
        onboardingUtils.isValidPhase(createIntervenientDTO.getAccountPhase(), ADD_INTERVENIENT);

        AccountRef accountRef = onboardingUtils.verifyIfAccountExists(createIntervenientDTO.getAccountNumber());

        Customer customer = createCustomerOrAddRelationOrAddInterventionToExistingOne(createIntervenientDTO, createIntervenientDTO.getIntervenient(),
                createIntervenientDTO.getCustomerNumber(), ADD_INTERVENIENT, accountRef);

        return CustomerMapper.INSTANCE.toCustomerDTO(customer);
    }

    @Override
    public CustomerDTO createRelationOrAddRelation(CreateRelationDTO createRelationDTO) {
        onboardingUtils.isValidPhase(createRelationDTO.getAccountPhase(), ADD_REL);

        AccountRef accountRef = onboardingUtils.verifyIfAccountExists(createRelationDTO.getAccountNumber());

        String childNumber = createRelationDTO.getChildCustomerNumber();
        if(customerRepoService.getCustomerByNumber(childNumber) == null)
            throw new OnboardingException("Não é possível inserir uma relação para com o cliente " + childNumber);

        Customer customer = createCustomerOrAddRelationOrAddInterventionToExistingOne(createRelationDTO, createRelationDTO.getParentCustomer(),
                createRelationDTO.getParentCustomerNumber(), ADD_REL, accountRef);

        return CustomerMapper.INSTANCE.toCustomerDTO(customer);
    }

    private Customer createCustomerOrAddRelationOrAddInterventionToExistingOne(Object request, CustomerRequestDTO parentCustomer, String parentCustomerNumber, OperationType operationType, AccountRef accountRef) {
        String accountId = accountRef.getId();
        Customer customer;
        CustomerRefDTO customerRefDTO;

        if(parentCustomer != null) {
            validateCustomer(parentCustomer, null, operationType);

            customer = createNewCustomer(parentCustomer, accountId);
            customerRefDTO = CustomerRefDTO.builder().customerId(customer.getId()).customerNumber(customer.getNumber()).build();

            kafkaProducer.sendEvent(documentTopicName, UPDATE_CUSTOMER_REF, customerRefDTO);
            kafkaProducer.sendEvent(accountTopicName, UPDATE_CUSTOMER_REF , customerRefDTO);
            if(ADD_INTERVENIENT.equals(operationType)) kafkaProducer.sendEvent(relationTopicName, UPDATE_CUSTOMER_REF , customerRefDTO);
        }else if(parentCustomerNumber != null){
            customer = customerRepoService.getCustomerByNumber(parentCustomerNumber);

            if(customer.getAccounts().stream().anyMatch(accountIdentifier -> accountIdentifier.getAccountId().equals(accountId)))
                throw new OnboardingException("O cliente não é válido para a conta!");

            customerRefDTO = CustomerRefDTO.builder().customerId(customer.getId()).customerNumber(customer.getNumber()).build();

        }else{
            String message = ADD_INTERVENIENT.equals(operationType)
                    ? "Não é possível criar o novo cliente ou adicionar a nova interveção. Necessita de introduzir um novo cliente ou o número de um cliente já existente!"
                    : "Não é possível criar o novo cliente ou adicionar a nova relação. Necessita de introduzir um novo cliente ou o número de um cliente já existente!";

            throw new OnboardingException(message);
        }

        if(ADD_INTERVENIENT.equals(operationType)){
            kafkaProducer.sendEvent(interventionTopicName, operationType, CreateIntervenientEvent.builder()
                    .createIntervenientDTO((CreateIntervenientDTO) request)
                    .accountRefDTO(AccountMapper.INSTANCE.toAccountRefDTO(accountRef))
                    .customerRefDTO(customerRefDTO)
                    .build());

        } else{
            kafkaProducer.sendEvent(relationTopicName, operationType, CreateRelationEvent.builder()
                    .createRelationDTO((CreateRelationDTO) request)
                    .accountRefDTO(AccountMapper.INSTANCE.toAccountRefDTO(accountRef))
                    .customerRefDTO(customerRefDTO)
                    .build());
        }

        return customer;
    }

    private Customer buildUpdatedCustomer(Customer customer, UpdateCustomerRequestDTO updateCustomerRequestDTO) {
        DocumentIdDTO documentIdDTO = updateCustomerRequestDTO.getDocumentId();
        TaxIdDTO taxIdDTO = updateCustomerRequestDTO.getTaxId();

        validateDocId(documentIdDTO, null, OperationType.UPDATE_CUSTOMER);
        validateTaxId(taxIdDTO, null, OperationType.UPDATE_CUSTOMER);

        List<AddressIdentifier> addressIdentifiers = updateCustomerAddresses(updateCustomerRequestDTO.getAddresses());
        List<ContactIdentifier> contactIdentifiers = updateCustomerContacts(updateCustomerRequestDTO.getContacts());

        customer.setAddresses(addressIdentifiers);
        customer.setAnnualIncome(updateCustomerRequestDTO.getAnnualIncome());
        customer.setBirthDate(updateCustomerRequestDTO.getBirthDate());
        customer.setContacts(contactIdentifiers);
        customer.setDocumentIdCountry(documentIdDTO.getDocumentIdCountry());
        customer.setDocumentIdNumber(documentIdDTO.getDocumentIdNumber());
        customer.setDocumentIdType(documentIdDTO.getDocumentIdType());
        customer.setDocumentIdExpirationDate(documentIdDTO.getDocumentIdExpirationDate());
        customer.setEducationLevel(updateCustomerRequestDTO.getEducationLevel());
        customer.setFatherName(updateCustomerRequestDTO.getFatherName());
        customer.setFirstName(updateCustomerRequestDTO.getFirstName());
        customer.setGender(updateCustomerRequestDTO.getGender());
        customer.setLastName(updateCustomerRequestDTO.getLastName());
        customer.setMotherName(updateCustomerRequestDTO.getMotherName());
        customer.setNationality(updateCustomerRequestDTO.getNationality());
        customer.setProfession(updateCustomerRequestDTO.getProfession());
        customer.setTaxIdCountry(taxIdDTO.getTaxIdCountry());
        customer.setTaxIdNumber(taxIdDTO.getTaxIdNumber());
        customer.setTaxIdType(taxIdDTO.getTaxIdType());

        return customer;
    }

    private List<ContactIdentifier> updateCustomerContacts(List<ContactDTO> contacts) {
        List<ContactIdentifier> contactIdentifiers = new ArrayList<>();

        if(!contacts.isEmpty()){
            contacts.forEach(contactDTO -> {
                validateContact(contactDTO, null, OperationType.UPDATE_CUSTOMER);
                Contact contact = contactRepoService.saveContactDB(Contact.builder()
                        .type(contactDTO.getType())
                        .value(contactDTO.getValue())
                        .creationTime(LocalDateTime.now())
                        .lastUpdateTime(LocalDateTime.now())
                        .build());
                contactIdentifiers.add(ContactIdentifier.builder().contactId(contact.getId()).build());
            });
        }

        return contactIdentifiers;
    }

    private List<AddressIdentifier> updateCustomerAddresses(List<AddressDTO> addresses) {
        List<AddressIdentifier> addressIdentifiers = new ArrayList<>();

        if(!addresses.isEmpty()){
            addresses.forEach(addressDTO -> {
                Address address = addressRepoService.saveAddressDB(Address.builder()
                        .city(addressDTO.getCity())
                        .country(addressDTO.getCountry())
                        .street(addressDTO.getStreet())
                        .zip(addressDTO.getZip())
                        .build());
                addressIdentifiers.add(AddressIdentifier.builder().addressId(address.getId()).build());
            });
        }

        return addressIdentifiers;
    }

    private void validateCustomer(CustomerRequestDTO customerRequestDTO, AccountRefDTO accountRefDTO, OperationType operationType) {
        validateContact(customerRequestDTO.getCustomerContact(), accountRefDTO, operationType);
        validateDocId(customerRequestDTO.getCustomerDocId(), accountRefDTO, operationType);
        validateTaxId(customerRequestDTO.getCustomerTaxId(), accountRefDTO, operationType);
        if(accountRefDTO != null){
            if(StringUtils.isBlank(customerRequestDTO.getCustomerBirthDate().toLocalDate().toString()) ||
                    StringUtils.isBlank(customerRequestDTO.getCustomerFirstName()) ||
                    StringUtils.isBlank(customerRequestDTO.getCustomerLastName())){

                onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);
                throw new OnboardingException("Houve um problema com o seu pedido, por favor verifique as suas informações enviadas!");
            } else if (!CUSTOMER_TYPES.contains(Optional.ofNullable(customerRequestDTO.getCustomerType()).orElse(""))) {
                onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);
                throw new OnboardingException("O tipo de cliente inserido não existe, tente novamente!");
            }
        }
    }

    private void validateContact(ContactDTO customerContact, AccountRefDTO accountRefDTO, OperationType operationType) {
        String customerContactType = Optional.ofNullable(customerContact).map(ContactDTO::getType).orElse("");
        String customerContactValue = Optional.ofNullable(customerContact).map(ContactDTO::getValue).orElse("");

        if (accountRefDTO != null && (!CONTACT_TYPES.contains(customerContactType) || (TELEPHONE.name().equals(customerContactType) &&
                    !Pattern.compile("^(\\d{3}[ ]?){2}\\d{3}$").matcher(customerContactValue).matches()) ||
                    (EMAIL.name().equals(customerContactType) && !Pattern.compile("^(.+)@(\\S+) $").matcher(customerContactValue).matches()))){
                onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);
                throw new OnboardingException("O tipo de contacto inserido não é válido!");
            }

    }
    private void validateDocId(DocumentIdDTO customerDocId, AccountRefDTO accountRefDTO, OperationType operationType) {
        String customerDocIdNumber = Optional.ofNullable(customerDocId).map(DocumentIdDTO::getDocumentIdNumber).orElse("").trim();
        LocalDateTime actualTime = LocalDateTime.now();
        LocalDateTime customerDocIdExpirationdate = Optional.ofNullable(customerDocId).map(DocumentIdDTO::getDocumentIdExpirationDate).orElse(actualTime);

        if (accountRefDTO != null && (!DOCUMENT_TYPES_CREATE_ACCOUNT_REQUEST.contains(Optional.ofNullable(customerDocId).map(DocumentIdDTO::getDocumentIdType).orElse("")) ||
                    !Pattern.compile("^\\d{8} \\d [A-Z]{2}\\d$").matcher(customerDocIdNumber).matches() ||
                    Boolean.FALSE.equals(customerDocIdExpirationdate.toLocalDate().isAfter(actualTime.toLocalDate())))){
                onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);
                throw new OnboardingException("Ocorreu um erro. Verifique os campos do documento de identificação!");
            }

    }
    private void validateTaxId(TaxIdDTO customerTaxId, AccountRefDTO accountRefDTO, OperationType operationType) {
        String customerDocIdNumber = Optional.ofNullable(customerTaxId).map(TaxIdDTO::getTaxIdNumber).orElse("").trim();

        if (accountRefDTO != null && (!DOCUMENT_TYPES_CREATE_ACCOUNT_REQUEST.contains(Optional.ofNullable(customerTaxId).map(TaxIdDTO::getTaxIdType).orElse("")) ||
                    !Pattern.compile("^\\d{9}$").matcher(customerDocIdNumber).matches())){
                onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);
                throw new OnboardingException("O tipo de documento inserido não é valido!");
            }

    }
}
