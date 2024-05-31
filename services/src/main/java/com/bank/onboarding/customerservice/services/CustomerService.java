package com.bank.onboarding.customerservice.services;

import com.bank.onboarding.commonslib.utils.kafka.CreateAccountEvent;
import com.bank.onboarding.commonslib.utils.kafka.ErrorEvent;

public interface CustomerService {
 void createCustomerForCreateAccountOperation(CreateAccountEvent createAccountEvent);
 void handleErrorEvent(ErrorEvent errorEvent);
}
