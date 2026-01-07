## 🧩 Customer Service

The Customer Service is a core microservice responsible for managing customer-related data and business logic within a distributed banking onboarding system. It encapsulates all operations related to customer creation, updates, relationships, and interactions with accounts, acting as the authoritative source of customer information.

Designed following Domain-Driven Design (DDD) principles, this service operates independently with its own database and communicates asynchronously with other services using event streaming. It plays a central role in coordinating customer state across the system during the bank account opening process.

## 🔍 Key Features

- Creation and validation of customer profiles
- Management of customer personal data, addresses, and contacts
- Handling customer interventions and relationships with accounts
- Event-driven communication with other microservices
- Consistency maintenance across distributed services via Kafka events

## 🔗 API Endpoints
- PUT /customers/{customerNumber} – Update customers information
- PUT /intervention - Add interventions in the account.
- PUT /relation - Add customers relationships in the account.

## 👨‍💻 Technologies

<div style="display: inline_block"><br>
<img align="center" alt="Java" height="40" width="40" src="https://github.com/devicons/devicon/blob/master/icons/java/java-original.svg">
<img align="center" alt="Spring" height="40" width="40" src="https://github.com/devicons/devicon/blob/master/icons/spring/spring-original.svg">
<img align="center" alt="Docker" height="40" width="40" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/docker/docker-original.svg" />
<img align="center" alt="PostgreSQL" height="40" width="40" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/postgresql/postgresql-original.svg" />
</div>

## 📂 Repository Structure

The repository is organized as follows:

- `boot`: Module that includes the application startup.
- `services/src/main/java/com/bank/onboarding/accountservice/services`: Contains services and their implementation.
- `web/src/main/java/com/bank/onboarding/accountservice/controllers`: Contains all the controllers of the application.

## 📋 Prerequisites

- Java 17+
- Maven
- Docker
- PostgreSQL database instance (local or containerized)

## 🌟 Additional Resources

- [Master's dissertation](http://hdl.handle.net/10400.22/26586)
