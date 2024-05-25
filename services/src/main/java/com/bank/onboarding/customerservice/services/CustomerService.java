package com.bank.onboarding.customerservice.services;

import com.bank.onboarding.commonslib.utils.kafka.CreateAccountEvent;

public interface CustomerService {
 void createCustomerForCreateAccountOperation(CreateAccountEvent createAccountEvent);
}
