FROM maven:3.9.11-eclipse-temurin-17 AS builder
WORKDIR /workspace
COPY pom.xml .
RUN mvn -B -ntp dependency:go-offline
COPY src ./src
RUN mvn -B -ntp clean package

FROM flink:1.20.5-scala_2.12-java17
COPY --from=builder /workspace/target/flink-banking-fraud-lab.jar /opt/flink/usrlib/flink-banking-fraud-lab.jar
