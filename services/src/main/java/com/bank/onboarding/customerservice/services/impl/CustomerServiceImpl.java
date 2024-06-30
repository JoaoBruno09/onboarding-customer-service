package com.bank.onboarding.customerservice.services.impl;

import com.bank.onboarding.commonslib.persistence.enums.OperationType;
import com.bank.onboarding.commonslib.persistence.exceptions.OnboardingException;
import com.bank.onboarding.commonslib.persistence.models.Address;
import com.bank.onboarding.commonslib.persistence.models.Contact;
import com.bank.onboarding.commonslib.persistence.models.Customer;
import com.bank.onboarding.commonslib.persistence.models.identifiers.AccountIdentifier;
import com.bank.onboarding.commonslib.persistence.models.identifiers.AddressIdentifier;
import com.bank.onboarding.commonslib.persistence.models.identifiers.ContactIdentifier;
import com.bank.onboarding.commonslib.persistence.services.AddressRepoService;
import com.bank.onboarding.commonslib.persistence.services.ContactRepoService;
import com.bank.onboarding.commonslib.persistence.services.CustomerRepoService;
import com.bank.onboarding.commonslib.utils.AsyncExecutor;
import com.bank.onboarding.commonslib.utils.OnboardingUtils;
import com.bank.onboarding.commonslib.utils.kafka.KafkaProducer;
import com.bank.onboarding.commonslib.utils.kafka.models.CardAndNetbancoEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.CreateAccountEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.CreateIntervenientEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.CreateRelationEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.DocUploadEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.ErrorEvent;
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
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.ValidationUtils;
import org.springframework.validation.Validator;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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
import static com.bank.onboarding.commonslib.persistence.enums.OperationType.DELETE_INTERVENIENT;
import static com.bank.onboarding.commonslib.persistence.enums.OperationType.DELETE_REL;
import static com.bank.onboarding.commonslib.persistence.enums.OperationType.UPDATE_CUSTOMER_REF;

@Slf4j
@Service
@RequiredArgsConstructor
public class CustomerServiceImpl implements CustomerService {

    private final CustomerRepoService customerRepoService;
    private final ContactRepoService contactRepoService;
    private final AddressRepoService addressRepoService;
    private final KafkaProducer kafkaProducer;
    private final OnboardingUtils onboardingUtils;
    private final Validator validator;
    private final AsyncExecutor asyncExecutor;

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
        Customer customer = createNewCustomer(customerRequestDTO, createAccountEvent.getAccountRefDTO().getAccountNumber(), CREATE_ACCOUNT);

        CustomerRefDTO customerRefDTO = CustomerRefDTO.builder().customerNumber(customer.getNumber()).isValid(false).accounts(customer.getAccounts()).build();
        createAccountEvent.setCustomerRefDTO(customerRefDTO);

        List<CompletableFuture<?>> completableFutureList = new ArrayList<>();
        completableFutureList.add(CompletableFuture.runAsync(() -> kafkaProducer.sendEvent(interventionTopicName, CREATE_ACCOUNT, createAccountEvent)));
        completableFutureList.add(CompletableFuture.runAsync(() -> kafkaProducer.sendEvent(documentTopicName, CREATE_ACCOUNT, createAccountEvent)));
        completableFutureList.add(CompletableFuture.runAsync(() -> kafkaProducer.sendEvent(accountTopicName, UPDATE_CUSTOMER_REF , customerRefDTO)));

        asyncExecutor.execute(completableFutureList);
    }

    @Override
    public void handleErrorEvent(ErrorEvent errorEvent) {
        OperationType operationType = errorEvent.getOperationType();
        if(CREATE_ACCOUNT.equals(operationType)){
            Customer customerToDeleteAccount = customerRepoService.getCustomerByNumber(errorEvent.getCustomerRefDTO().getCustomerNumber());
            customerToDeleteAccount.setAccounts(customerToDeleteAccount.getAccounts().stream().filter(accountIdentifier ->
                    !errorEvent.getAccountRefDTO().getAccountNumber().equals(accountIdentifier.getAccountNumber())).toList());

            customerRepoService.saveCustomerDB(customerToDeleteAccount);
        } else if (ADD_INTERVENIENT.equals(operationType) || DELETE_INTERVENIENT.equals(operationType) || ADD_REL.equals(operationType) || DELETE_REL.equals(operationType)){
            String customerNumber = errorEvent.getCustomerRefDTO().getCustomerNumber();
            if(Boolean.TRUE.equals(errorEvent.getIsNewCustomer())){
                customerRepoService.deleteCustomerByNumber(customerNumber);
            }else{
                Customer customerToUpdateIndicator = customerRepoService.getCustomerByNumber(customerNumber);
                if (ADD_INTERVENIENT.equals(operationType) || DELETE_INTERVENIENT.equals(operationType)) {
                    customerToUpdateIndicator.setIntervenientIndicator(false);
                } else{
                    customerToUpdateIndicator.setRelationIndicator(false);
                }
                customerToUpdateIndicator.setIsValid(false);
                customerRepoService.saveCustomerDB(customerToUpdateIndicator);

                kafkaProducer.sendEvent(accountTopicName, UPDATE_CUSTOMER_REF, CustomerRefDTO.builder()
                        .customerNumber(customerNumber)
                        .isValid(customerToUpdateIndicator.getIsValid())
                        .accounts(customerToUpdateIndicator.getAccounts()).build());
            }
        }
    }

    @Override
    public CustomerDTO updateCustomer(String customerNumber, UpdateCustomerRequestDTO updateCustomerRequestDTO) {
        if(Stream.of(updateCustomerRequestDTO).anyMatch(Objects::isNull))
            throw new OnboardingException("Houve um problema com o seu pedido, por favor verifique as suas informações enviadas!");

        Customer existingCustomer = customerRepoService.getCustomerByNumber(customerNumber);
        if(existingCustomer.getAccounts().stream().noneMatch(accountIdentifier ->
                updateCustomerRequestDTO.getAccountNumber().equals(accountIdentifier.getAccountNumber())))
                    throw new OnboardingException("O cliente que está a tentar atualizar não pertence à conta introduzida");

        onboardingUtils.isValidPhase(updateCustomerRequestDTO.getAccountPhase(), OperationType.UPDATE_CUSTOMER);

        buildUpdatedCustomer(existingCustomer, updateCustomerRequestDTO);

        CustomerDTO customerUpdated = CustomerMapper.INSTANCE.toCustomerDTO(existingCustomer);
        customerUpdated.setAddresses(updateCustomerRequestDTO.getAddresses());
        customerUpdated.setContacts(updateCustomerRequestDTO.getContacts());

        BeanPropertyBindingResult bindingCustomerValidations = new BeanPropertyBindingResult(customerUpdated, "customerDTO");
        ValidationUtils.invokeValidator(validator, customerUpdated, bindingCustomerValidations);

        if (!bindingCustomerValidations.hasErrors()){
            existingCustomer.setIsValid(true);
            kafkaProducer.sendEvent(accountTopicName, UPDATE_CUSTOMER_REF, CustomerRefDTO.builder()
                    .customerNumber(customerNumber).isValid(existingCustomer.getIsValid()).accounts(existingCustomer.getAccounts()));
        }

        return CustomerMapper.INSTANCE.toCustomerDTO(customerRepoService.saveCustomerDB(existingCustomer));
    }

    @Override
    public void updateCardCustomer(CardAndNetbancoEvent cardAndNetbancoEvent) {
        Customer customer = customerRepoService.getCustomerByNumber(cardAndNetbancoEvent.getCustomerNumber());
        customer.setCardIndicator(cardAndNetbancoEvent.isValue());
        customerRepoService.saveCustomerDB(customer);
    }

    @Override
    public void updateNetbancoCustomer(CardAndNetbancoEvent cardAndNetbancoEvent) {
        Customer customer = customerRepoService.getCustomerByNumber(cardAndNetbancoEvent.getCustomerNumber());
        customer.setOnlineBankingIndicator(cardAndNetbancoEvent.isValue());
        customerRepoService.saveCustomerDB(customer);
    }

    @Override
    public CustomerDTO createIntervenientOrAddIntervention(String customerNumber, CreateIntervenientDTO createIntervenientDTO) {
        onboardingUtils.isValidPhase(createIntervenientDTO.getAccountPhase(), ADD_INTERVENIENT);

        Customer customer = createCustomerOrAddRelationOrAddInterventionToExistingOne(createIntervenientDTO, createIntervenientDTO.getIntervenient(),
                customerNumber, createIntervenientDTO.getAccountNumber(), ADD_INTERVENIENT);

        return CustomerMapper.INSTANCE.toCustomerDTO(customer);
    }

    @Override
    public CustomerDTO createRelationOrAddRelation(String parentCustomerNumber, CreateRelationDTO createRelationDTO) {
        onboardingUtils.isValidPhase(createRelationDTO.getAccountPhase(), ADD_REL);

        String childNumber = createRelationDTO.getChildCustomerNumber();
        if(customerRepoService.getCustomerByNumber(childNumber) == null)
            throw new OnboardingException("Não é possível inserir uma relação para com o cliente " + childNumber);

        Customer customer = createCustomerOrAddRelationOrAddInterventionToExistingOne(createRelationDTO, createRelationDTO.getParentCustomer(),
                parentCustomerNumber, createRelationDTO.getAccountNumber(), ADD_REL);

        return CustomerMapper.INSTANCE.toCustomerDTO(customer);
    }

    @Override
    public void updateDocsValidOrNotValid(DocUploadEvent docUploadEvent) {
        Customer customer = customerRepoService.getCustomerByNumber(docUploadEvent.getCustomerNumber());
        customer.setIsValid(docUploadEvent.isAreDocsValid());
        customerRepoService.saveCustomerDB(customer);
        kafkaProducer.sendEvent(accountTopicName, UPDATE_CUSTOMER_REF, CustomerRefDTO.builder()
                .customerNumber(customer.getNumber()).isValid(customer.getIsValid()).accounts(customer.getAccounts()));
    }

    private Customer createNewCustomer(CustomerRequestDTO customerRequestDTO, String accountNumber, OperationType operationType) {
        Contact contact = contactRepoService.saveContactDB(Contact.builder()
                .type(customerRequestDTO.getCustomerContact().getType())
                .value(customerRequestDTO.getCustomerContact().getValue())
                .creationTime(LocalDateTime.now())
                .lastUpdateTime(LocalDateTime.now())
                .build());

        String customerNumber;
        do {
            customerNumber = "C" + ((int) faker.number().randomNumber(9, true));
        }while (!customerRepoService.getCustomersByNumber(customerNumber).isEmpty());

        Customer customer = Customer.builder()
                .accounts(List.of(AccountIdentifier.builder().accountNumber(accountNumber).build()))
                .birthDate(customerRequestDTO.getCustomerBirthDate())
                .contacts(List.of(ContactIdentifier.builder().contactId(contact.getId()).build()))
                .creationTime(LocalDateTime.now())
                .documentIdCountry(customerRequestDTO.getCustomerDocId().getDocumentIdCountry())
                .documentIdNumber(customerRequestDTO.getCustomerDocId().getDocumentIdNumber())
                .documentIdType(customerRequestDTO.getCustomerDocId().getDocumentIdType())
                .documentIdExpirationDate(customerRequestDTO.getCustomerDocId().getDocumentIdExpirationDate())
                .firstName(customerRequestDTO.getCustomerFirstName())
                .isValid(false)
                .lastName(customerRequestDTO.getCustomerLastName())
                .lastUpdateTime(LocalDateTime.now())
                .nationality("Português")
                .number(customerNumber)
                .taxIdCountry(customerRequestDTO.getCustomerTaxId().getTaxIdCountry())
                .taxIdNumber(customerRequestDTO.getCustomerTaxId().getTaxIdNumber())
                .taxIdType(customerRequestDTO.getCustomerTaxId().getTaxIdType())
                .type(customerRequestDTO.getCustomerType())
                .build();

        if(List.of(CREATE_ACCOUNT, ADD_INTERVENIENT).contains(operationType)) customer.setIntervenientIndicator(true);
        if(ADD_REL.equals(operationType)) customer.setRelationIndicator(true);

       return customerRepoService.saveCustomerDB(customer);
    }



    private Customer createCustomerOrAddRelationOrAddInterventionToExistingOne(Object request, CustomerRequestDTO customerRequest, String parentCustomerNumber, String accountNumber, OperationType operationType) {
        Customer customer;
        CustomerRefDTO customerRefDTO;
        boolean newCustomer;

        if(customerRequest != null) {
            validateCustomer(customerRequest, null, operationType);

            customer = createNewCustomer(customerRequest, accountNumber, operationType);
            customerRefDTO = CustomerRefDTO.builder().customerNumber(customer.getNumber()).isValid(false).accounts(customer.getAccounts()).build();
            newCustomer = true;

            kafkaProducer.sendEvent(accountTopicName, UPDATE_CUSTOMER_REF , customerRefDTO);

        }else if(parentCustomerNumber != null){
            customer = customerRepoService.getCustomerByNumber(parentCustomerNumber);
            newCustomer = false;

            if(customer.getAccounts().stream().noneMatch(accountIdentifier -> accountIdentifier.getAccountNumber().equals(accountNumber)))
                throw new OnboardingException("O cliente não é válido para a conta!");

            if(ADD_INTERVENIENT.equals(operationType) && Boolean.FALSE.equals(Optional.ofNullable(customer.getIntervenientIndicator()).orElse(true)))
                customer.setIntervenientIndicator(true);

            if(ADD_REL.equals(operationType) && Boolean.FALSE.equals(Optional.ofNullable(customer.getRelationIndicator()).orElse(true)))
                customer.setRelationIndicator(true);

            customerRefDTO = CustomerRefDTO.builder().customerNumber(customer.getNumber()).customerNumber(customer.getNumber()).accounts(customer.getAccounts()).build();
        }else{
            String message = ADD_INTERVENIENT.equals(operationType)
                    ? "Não é possível criar o novo cliente ou adicionar a nova interveção. Necessita de introduzir um novo cliente ou o número de um cliente já existente!"
                    : "Não é possível criar o novo cliente ou adicionar a nova relação. Necessita de introduzir um novo cliente ou o número de um cliente já existente!";

            throw new OnboardingException(message);
        }

        if(ADD_INTERVENIENT.equals(operationType)){
            kafkaProducer.sendEvent(interventionTopicName, operationType, CreateIntervenientEvent.builder()
                    .createIntervenientDTO((CreateIntervenientDTO) request)
                    .newCustomer(newCustomer)
                    .accountRefDTO(AccountRefDTO.builder().accountNumber(accountNumber).build())
                    .customerRefDTO(customerRefDTO)
                    .build());
        } else{
            kafkaProducer.sendEvent(relationTopicName, operationType, CreateRelationEvent.builder()
                    .createRelationDTO((CreateRelationDTO) request)
                    .newCustomer(newCustomer)
                    .accountRefDTO(AccountRefDTO.builder().accountNumber(accountNumber).build())
                    .customerRefDTO(customerRefDTO)
                    .build());
        }

        return customer;
    }

    private void buildUpdatedCustomer(Customer customer, UpdateCustomerRequestDTO updateCustomerRequestDTO) {
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

                if(CREATE_ACCOUNT.equals(operationType)) onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);

                throw new OnboardingException("Houve um problema com o seu pedido, por favor verifique as suas informações enviadas!");
            } else if (!CUSTOMER_TYPES.contains(Optional.ofNullable(customerRequestDTO.getCustomerType()).orElse(""))) {

                if(CREATE_ACCOUNT.equals(operationType)) onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);

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

                if(CREATE_ACCOUNT.equals(operationType)) onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);

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

                if(CREATE_ACCOUNT.equals(operationType)) onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);

                throw new OnboardingException("Ocorreu um erro. Verifique os campos do documento de identificação!");
            }

    }

    private void validateTaxId(TaxIdDTO customerTaxId, AccountRefDTO accountRefDTO, OperationType operationType) {
        String customerDocIdNumber = Optional.ofNullable(customerTaxId).map(TaxIdDTO::getTaxIdNumber).orElse("").trim();

        if (accountRefDTO != null && (!DOCUMENT_TYPES_CREATE_ACCOUNT_REQUEST.contains(Optional.ofNullable(customerTaxId).map(TaxIdDTO::getTaxIdType).orElse("")) ||
                    !Pattern.compile("^\\d{9}$").matcher(customerDocIdNumber).matches())){

                if(CREATE_ACCOUNT.equals(operationType)) onboardingUtils.sendErrorEvent(accountTopicName, accountRefDTO, null, operationType);

                throw new OnboardingException("O tipo de documento inserido não é valido!");
            }

    }
}
