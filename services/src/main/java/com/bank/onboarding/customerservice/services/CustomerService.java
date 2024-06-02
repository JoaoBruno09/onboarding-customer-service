package com.bank.onboarding.customerservice.services;

import com.bank.onboarding.commonslib.utils.kafka.CreateAccountEvent;
import com.bank.onboarding.commonslib.utils.kafka.ErrorEvent;
import com.bank.onboarding.commonslib.web.dtos.customer.CustomerDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.UpdateCustomerRequestDTO;

public interface CustomerService {
 void createCustomerForCreateAccountOperation(CreateAccountEvent createAccountEvent);
 void handleErrorEvent(ErrorEvent errorEvent);
 CustomerDTO updateCustomer(String customerNumber, UpdateCustomerRequestDTO updateCustomerRequestDTO);
}
