package com.bank.onboarding.customerservice.controllers;

import com.bank.onboarding.commonslib.persistence.models.Address;
import com.bank.onboarding.commonslib.persistence.models.Contact;
import com.bank.onboarding.commonslib.persistence.models.Customer;
import com.bank.onboarding.commonslib.persistence.services.AddressRepoService;
import com.bank.onboarding.commonslib.persistence.services.ContactRepoService;
import com.bank.onboarding.commonslib.persistence.services.CustomerRepoService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("customer")
@RequiredArgsConstructor
public class CustomerController {
    private final CustomerRepoService customerRepoService;
    private final ContactRepoService contactRepoService;
    private final AddressRepoService addressRepoService;

    @GetMapping("/test/customers")
    public List<Customer> getCustomers() {
        return customerRepoService.getAllCustomers();
    }

    @GetMapping("/test/contacts")
    public List<Contact> getContacts() {
        return contactRepoService.getAllContacts();
    }

    @GetMapping("/test/addresses")
    public List<Address> getAddresses() {
        return addressRepoService.getAllAddresses();
    }
}
