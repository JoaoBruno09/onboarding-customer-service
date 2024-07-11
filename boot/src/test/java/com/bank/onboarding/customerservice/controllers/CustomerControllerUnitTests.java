package com.bank.onboarding.customerservice.controllers;

import com.bank.onboarding.commonslib.persistence.repositories.AccountRepository;
import com.bank.onboarding.commonslib.persistence.repositories.AddressRepository;
import com.bank.onboarding.commonslib.persistence.repositories.CardRepository;
import com.bank.onboarding.commonslib.persistence.repositories.ContactRepository;
import com.bank.onboarding.commonslib.persistence.repositories.CustomerRefRepository;
import com.bank.onboarding.commonslib.persistence.repositories.CustomerRepository;
import com.bank.onboarding.commonslib.persistence.repositories.DocumentRepository;
import com.bank.onboarding.commonslib.persistence.repositories.InterventionRepository;
import com.bank.onboarding.commonslib.persistence.repositories.RelationRepository;
import com.bank.onboarding.commonslib.persistence.services.AccountRepoService;
import com.bank.onboarding.commonslib.persistence.services.CardRepoService;
import com.bank.onboarding.commonslib.persistence.services.CustomerRefRepoService;
import com.bank.onboarding.commonslib.utils.OnboardingUtils;
import com.bank.onboarding.commonslib.utils.mappers.CustomerMapper;
import com.bank.onboarding.commonslib.web.SecurityConfig;
import com.bank.onboarding.commonslib.web.dtos.customer.CreateIntervenientDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CreateRelationDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.CustomerDTO;
import com.bank.onboarding.commonslib.web.dtos.customer.UpdateCustomerRequestDTO;
import com.bank.onboarding.customerservice.services.CustomerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;

import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildCreateIntervenientDTO;
import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildCreateRelationDTO;
import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildCustomer;
import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildUpdateCustomerRequestDTO;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(SpringExtension.class)
@WebMvcTest(CustomerController.class)
@Import(SecurityConfig.class)
@MockBeans({
        @MockBean(OnboardingUtils.class),
        @MockBean(AccountRepoService.class),
        @MockBean(CardRepoService.class),
        @MockBean(CustomerRefRepoService.class),
        @MockBean(CustomerRefRepository.class),
        @MockBean(CustomerRepository.class),
        @MockBean(AccountRepository.class),
        @MockBean(CardRepository.class),
        @MockBean(DocumentRepository.class),
        @MockBean(InterventionRepository.class),
        @MockBean(RelationRepository.class),
        @MockBean(ContactRepository.class),
        @MockBean(AddressRepository.class)
})
class CustomerControllerUnitTests {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private SecurityConfig securityConfig;

    @MockBean
    private CustomerService customerService;

    @Value("${bank.onboarding.client.id}")
    private String clientId;

    private CustomerDTO customerDTO;
    private String token;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setUp() {
        token = securityConfig.generateJWToken();
        objectMapper.registerModule(new JavaTimeModule());

        customerDTO = CustomerMapper.INSTANCE.toCustomerDTO(buildCustomer("8040801240825", "C123456789"));
    }

    @Test
    void updateCustomerTest() throws Exception{
        UpdateCustomerRequestDTO updateCustomerRequestDTO = buildUpdateCustomerRequestDTO();

        when(customerService.updateCustomer("C123456789", updateCustomerRequestDTO)).thenReturn(customerDTO);
        mockMvc.perform(put("/customer/C123456789")
                        .header("Authorization", "Bearer " + token)
                        .header("X-Onboarding-Client-Id", clientId)
                        .param("customerNumber", "C123456789")
                        .content(objectMapper.writeValueAsString(updateCustomerRequestDTO))
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(jsonPath("$.firstName").value(customerDTO.getFirstName()))
                .andExpect(jsonPath("$.lastName").value(customerDTO.getLastName()))
                .andExpect(jsonPath("$.number").value(customerDTO.getNumber()))
                .andExpect(jsonPath("$.type").value(customerDTO.getType()));
    }

    @Test
    void createCustomerIntervenientTest() throws Exception{
        CreateIntervenientDTO createIntervenientDTO = buildCreateIntervenientDTO();

        when(customerService.createIntervenientOrAddIntervention("C123456789", createIntervenientDTO)).thenReturn(customerDTO);
        mockMvc.perform(put("/customer/intervention")
                        .header("Authorization", "Bearer " + token)
                        .header("X-Onboarding-Client-Id", clientId)
                        .param("customerNumber", "C123456789")
                        .content(objectMapper.writeValueAsString(createIntervenientDTO))
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(jsonPath("$.firstName").value(customerDTO.getFirstName()))
                .andExpect(jsonPath("$.lastName").value(customerDTO.getLastName()))
                .andExpect(jsonPath("$.number").value(customerDTO.getNumber()))
                .andExpect(jsonPath("$.type").value(customerDTO.getType()));
    }

    @Test
    void createCustomerRelationTest() throws Exception{
        CreateRelationDTO createRelationDTO = buildCreateRelationDTO();

        when(customerService.createRelationOrAddRelation("C123456789", createRelationDTO)).thenReturn(customerDTO);
        mockMvc.perform(put("/customer/relation")
                        .header("Authorization", "Bearer " + token)
                        .header("X-Onboarding-Client-Id", clientId)
                        .param("customerNumber", "C123456789")
                        .content(objectMapper.writeValueAsString(createRelationDTO))
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(jsonPath("$.firstName").value(customerDTO.getFirstName()))
                .andExpect(jsonPath("$.lastName").value(customerDTO.getLastName()))
                .andExpect(jsonPath("$.number").value(customerDTO.getNumber()))
                .andExpect(jsonPath("$.type").value(customerDTO.getType()));
    }
}
