FROM maven:3.9.6-amazoncorretto-21
WORKDIR /boot/target
COPY /boot/target/customer-service-0.0.1-SNAPSHOT.jar customer-service-0.0.1-SNAPSHOT.jar
ENTRYPOINT ["java", "-jar", "customer-service-0.0.1-SNAPSHOT.jar"]