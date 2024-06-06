package com.bank.onboarding.customerservice.controllers;

import com.bank.onboarding.commonslib.persistence.exceptions.OnboardingException;
import com.bank.onboarding.commonslib.web.dtos.customer.CreateIntervenientDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CreateRelationDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CustomerDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.UpdateCustomerRequestDTO;
import com.bank.onboarding.customerservice.services.CustomerService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("customer")
@RequiredArgsConstructor
public class CustomerController {
    private final CustomerService customerService;

    @PutMapping("/{customerNumber}")
    public ResponseEntity<?> updateCustomer(@PathVariable("customerNumber") String customerNumber,
                                            @RequestBody @Valid UpdateCustomerRequestDTO updateCustomerRequestDTO){
        try {
            final CustomerDTO customerDTO = customerService.updateCustomer(customerNumber, updateCustomerRequestDTO);
            return new ResponseEntity<>(customerDTO, HttpStatus.OK);
        }
        catch(OnboardingException e ) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        }
    }

    @PutMapping("/intervention")
    public ResponseEntity<?> createCustomerIntervenient(@RequestParam(name = "customerNumber", required = false) String customerNumber,
                                                        @RequestBody @Valid CreateIntervenientDTO createIntervenientDTO){
        try {
            final CustomerDTO customerDTO = customerService.createIntervenientOrAddIntervention(customerNumber, createIntervenientDTO);
            return new ResponseEntity<>(customerDTO, HttpStatus.OK);
        }
        catch(OnboardingException e ) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        }
    }

    @PutMapping("/relation")
    public ResponseEntity<?> createCustomerRelation(@RequestParam(name = "customerNumber", required = false) String parentCustomerNumber,
                                                    @RequestBody @Valid CreateRelationDTO createRelationDTO){
        try {
            final CustomerDTO customerDTO = customerService.createRelationOrAddRelation(parentCustomerNumber, createRelationDTO);
            return new ResponseEntity<>(customerDTO, HttpStatus.OK);
        }
        catch(OnboardingException e ) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        }
    }
}
