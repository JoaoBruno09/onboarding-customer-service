package com.bank.onboarding.customerservice;

import com.bank.onboarding.commonslib.persistence.repositories.CustomerRepository;
import com.bank.onboarding.commonslib.web.SecurityConfig;
import com.bank.onboarding.commonslib.web.dtos.customer.CustomerDTO;
import com.bank.onboarding.customerservice.services.CustomerService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildCreateIntervenientDTO;
import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildCreateRelationDTO;
import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildUpdateCustomerRequestDTO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CustomerApiIntegrationTests {
    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private CustomerRepository customerRepository;

    @Autowired
    private CustomerService customerService;

    @Autowired
    private SecurityConfig securityConfig;

    @Value("${bank.onboarding.client.id}")
    private String clientId;

    private String token;

    private HttpHeaders httpHeaders;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setUp() {
        token = securityConfig.generateJWToken();
        httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        httpHeaders.setBearerAuth(token);
        httpHeaders.set("X-Onboarding-Client-Id", clientId);
        objectMapper.registerModule(new JavaTimeModule());
    }

    private String createURLWithPort() {
        return "http://localhost:" + port + "/customers/";
    }

    @Test
    void updateCustomerTest() throws JsonProcessingException {
        HttpEntity<String> entity = new HttpEntity<>(objectMapper.writeValueAsString(buildUpdateCustomerRequestDTO()), httpHeaders);
        ResponseEntity<?> response = restTemplate.exchange(
                createURLWithPort() + "C123456789", HttpMethod.PUT, entity, CustomerDTO.class);

        assertEquals(response.getStatusCode(), HttpStatusCode.valueOf(200));
        //assertTrue(interventionRepository.findById(interventionId).isEmpty());
    }

    @Test
    void createCustomerIntervenient() throws JsonProcessingException{
        HttpEntity<String> entity = new HttpEntity<>(objectMapper.writeValueAsString(buildCreateIntervenientDTO()), httpHeaders);
        ResponseEntity<?> response = restTemplate.exchange(
                createURLWithPort() + "intervention", HttpMethod.PUT, entity, CustomerDTO.class, "C123456789");

        assertEquals(response.getStatusCode(), HttpStatusCode.valueOf(200));
        assertTrue(customerRepository.findByNumber("C123456789").getIntervenientIndicator());
    }

    @Test
    void createCustomerRelation() throws JsonProcessingException{
        HttpEntity<String> entity = new HttpEntity<>(objectMapper.writeValueAsString(buildCreateRelationDTO()), httpHeaders);
        ResponseEntity<?> response = restTemplate.exchange(
                createURLWithPort() + "relation", HttpMethod.PUT, entity, CustomerDTO.class, "C123456789");

        assertEquals(response.getStatusCode(), HttpStatusCode.valueOf(200));
        assertTrue(customerRepository.findByNumber("C123456789").getRelationIndicator());
    }
}
