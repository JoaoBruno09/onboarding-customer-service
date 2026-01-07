package com.bank.onboarding.customerservice;

import com.bank.onboarding.commonslib.persistence.repositories.CustomerRepository;
import com.bank.onboarding.commonslib.web.SecurityConfig;
import com.bank.onboarding.commonslib.web.dtos.customer.CustomerDTO;
import com.bank.onboarding.customerservice.services.CustomerService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
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

import java.util.Objects;
import java.util.stream.Stream;

import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildCreateIntervenientDTO;
import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildCreateRelationDTO;
import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildCustomer;
import static com.bank.onboarding.commonslib.utils.TestOnboardingUtils.buildUpdateCustomerRequestDTO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

        String IBAN = "PT50 0000 2927 8040 8012 4082 5";
        customerRepository.save(buildCustomer(IBAN.trim().replaceAll(" ", "").substring(IBAN.length()-19), "Contact123132131"));
    }

    @AfterEach
    public void setDown() {
        customerRepository.findAllByNumber("C123456789").forEach(customer -> customerRepository.delete(customer));
    }

    private String createURLWithPort() {
        return "http://localhost:" + port + "/customers/";
    }

    @Test
    void updateCustomerTest() throws JsonProcessingException {
        HttpEntity<String> entity = new HttpEntity<>(objectMapper.writeValueAsString(buildUpdateCustomerRequestDTO()), httpHeaders);
        ResponseEntity<?> response = restTemplate.exchange(
                createURLWithPort() + "C123456789", HttpMethod.PUT, entity, CustomerDTO.class);

        CustomerDTO customerDTO = (CustomerDTO) response.getBody();
        assertEquals(response.getStatusCode(), HttpStatusCode.valueOf(200));
        assert customerDTO != null;
        assertFalse(Stream.of(customerService.updateCustomer("C123456789", buildUpdateCustomerRequestDTO())).anyMatch(Objects::isNull));
        assertFalse(Stream.of(customerRepository.findByNumber(customerDTO.getNumber())).anyMatch(Objects::isNull));
    }

    @Test
    void createCustomerIntervenientTest() throws JsonProcessingException{
        HttpEntity<String> entity = new HttpEntity<>(objectMapper.writeValueAsString(buildCreateIntervenientDTO()), httpHeaders);
        ResponseEntity<?> response = restTemplate.exchange(
                createURLWithPort() + "intervention", HttpMethod.PUT, entity, CustomerDTO.class, "C123456789");

        CustomerDTO customerDTO = (CustomerDTO) response.getBody();
        assertEquals(response.getStatusCode(), HttpStatusCode.valueOf(200));
        assert customerDTO != null;
        assertTrue(customerDTO.getIntervenientIndicator());
        assertTrue(customerService.createIntervenientOrAddIntervention("C123456789", buildCreateIntervenientDTO()).getIntervenientIndicator());
        assertTrue(customerRepository.findByNumber(customerDTO.getNumber()).getIntervenientIndicator());
    }

    @Test
    void createCustomerRelationTest() throws JsonProcessingException{
        HttpEntity<String> entity = new HttpEntity<>(objectMapper.writeValueAsString(buildCreateRelationDTO()), httpHeaders);
        ResponseEntity<?> response = restTemplate.exchange(
                createURLWithPort() + "relation", HttpMethod.PUT, entity, CustomerDTO.class, "C123456789");

        CustomerDTO customerDTO = (CustomerDTO) response.getBody();
        assertEquals(response.getStatusCode(), HttpStatusCode.valueOf(200));
        assert customerDTO != null;
        assertTrue(customerDTO.getRelationIndicator());
        assertTrue(customerService.createRelationOrAddRelation("C123456789", buildCreateRelationDTO()).getRelationIndicator());
        assertTrue(customerRepository.findByNumber(customerDTO.getNumber()).getRelationIndicator());
    }
}
