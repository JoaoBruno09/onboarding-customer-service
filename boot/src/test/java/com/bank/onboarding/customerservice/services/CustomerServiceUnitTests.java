package com.bank.onboarding.customerservice.services;

import com.bank.onboarding.commonslib.persistence.enums.CustomerType;
import com.bank.onboarding.commonslib.utils.mappers.CustomerMapper;
import com.bank.onboarding.commonslib.web.dtos.customer.CreateIntervenientDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CreateRelationDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CustomerDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.UpdateCustomerRequestDTO;
import com.bank.onboarding.customerservice.Application;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildCreateIntervenientDTO;
import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildCreateRelationDTO;
import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildCustomer;
import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildUpdateCustomerRequestDTO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = Application.class)
@ExtendWith({SpringExtension.class})
class CustomerServiceUnitTests {

    @Mock
    private CustomerService customerService;

    private CustomerDTO customerDTO;

    @BeforeEach
    public void setUp() {
        customerDTO = CustomerMapper.INSTANCE.toCustomerDTO(buildCustomer("8040801240825", "C123456789"));
    }

    @Test
    void updateCustomerTest() throws Exception{
        UpdateCustomerRequestDTO updateCustomerRequestDTO = buildUpdateCustomerRequestDTO();

        when(customerService.updateCustomer("C123456789", updateCustomerRequestDTO)).thenReturn(customerDTO);

        assertEquals("Mário Ferreira", customerService.updateCustomer("C123456789", updateCustomerRequestDTO).getFirstName());
        assertEquals("Neves", customerService.updateCustomer("C123456789", updateCustomerRequestDTO).getLastName());
        assertEquals("C123456789", customerService.updateCustomer("C123456789", updateCustomerRequestDTO).getNumber());
        assertEquals(CustomerType.PARTICULAR.name(), customerService.updateCustomer("C123456789", updateCustomerRequestDTO).getType());
    }

    @Test
    void createCustomerIntervenientTest() throws Exception{
        CreateIntervenientDTO createIntervenientDTO = buildCreateIntervenientDTO();

        when(customerService.createIntervenientOrAddIntervention("C123456789", createIntervenientDTO)).thenReturn(customerDTO);

        assertEquals("Mário Ferreira", customerService.createIntervenientOrAddIntervention("C123456789", createIntervenientDTO).getFirstName());
        assertEquals("Neves", customerService.createIntervenientOrAddIntervention("C123456789", createIntervenientDTO).getLastName());
        assertEquals("C123456789", customerService.createIntervenientOrAddIntervention("C123456789", createIntervenientDTO).getNumber());
        assertEquals(CustomerType.PARTICULAR.name(), customerService.createIntervenientOrAddIntervention("C123456789", createIntervenientDTO).getType());
    }

    @Test
    void createCustomerRelationTest() throws Exception{
        CreateRelationDTO createRelationDTO = buildCreateRelationDTO();

        when(customerService.createRelationOrAddRelation("C123456789", createRelationDTO)).thenReturn(customerDTO);

        assertEquals("Mário Ferreira", customerService.createRelationOrAddRelation("C123456789", createRelationDTO).getFirstName());
        assertEquals("Neves", customerService.createRelationOrAddRelation("C123456789", createRelationDTO).getLastName());
        assertEquals("C123456789", customerService.createRelationOrAddRelation("C123456789", createRelationDTO).getNumber());
        assertEquals(CustomerType.PARTICULAR.name(), customerService.createRelationOrAddRelation("C123456789", createRelationDTO).getType());
    }
}
