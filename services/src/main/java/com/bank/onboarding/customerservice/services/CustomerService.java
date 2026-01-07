package com.bank.onboarding.customerservice.services;

import com.bank.onboarding.commonslib.utils.kafka.models.CardAndNetbancoEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.CreateAccountEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.DocUploadEvent;
import com.bank.onboarding.commonslib.utils.kafka.models.ErrorEvent;
import com.bank.onboarding.commonslib.web.dtos.customer.CreateIntervenientDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CreateRelationDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CustomerDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.UpdateCustomerRequestDTO;

public interface CustomerService {
 void createCustomerForCreateAccountOperation(CreateAccountEvent createAccountEvent);
 void handleErrorEvent(ErrorEvent errorEvent);
 CustomerDTO updateCustomer(String customerNumber, UpdateCustomerRequestDTO updateCustomerRequestDTO);
 void updateCardCustomer(CardAndNetbancoEvent cardAndNetbancoEvent);
 void updateNetbancoCustomer(CardAndNetbancoEvent cardAndNetbancoEvent);
 CustomerDTO createIntervenientOrAddIntervention(String customerNumber, CreateIntervenientDTO createIntervenientDTO);
 CustomerDTO createRelationOrAddRelation(String parentCustomerNumber, CreateRelationDTO createRelationDTO);
 void updateDocsValidOrNotValid(DocUploadEvent docUploadEvent);
}
