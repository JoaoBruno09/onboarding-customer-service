package com.bank.onboarding.customerservice.controllers;

import com.bank.onboarding.commonslib.persistence.models.Customer;
import com.bank.onboarding.commonslib.persistence.services.CustomerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("customer")
@RequiredArgsConstructor
public class CustomerController {
    private final CustomerService customerService;

    @GetMapping("/test")
    public List<Customer> index() {
        return customerService.getAllCustomers();
    }
}
