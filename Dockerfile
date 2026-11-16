# Stage 1: Build JAR and copy runtime dependencies
FROM maven:3.9.9-eclipse-temurin-21 AS builder
WORKDIR /build

COPY pom.xml .
COPY src ./src
COPY specs ./specs

RUN mvn -B clean package dependency:copy-dependencies -DincludeScope=runtime -DskipTests

# Stage 2: Minimal Temurin 21 JRE Runtime
FROM eclipse-temurin:21-jre

# Create non-root system user and group
RUN groupadd -g 10001 broker && \
    useradd -u 10001 -g broker -s /bin/bash -m broker

WORKDIR /app

# Copy application JAR, dependencies, and topology specs
COPY --from=builder --chown=broker:broker /build/target/distributed-message-broker-1.0.0-SNAPSHOT.jar /app/app.jar
COPY --from=builder --chown=broker:broker /build/target/dependency /app/lib
COPY --from=builder --chown=broker:broker /build/specs /app/specs

# Switch to non-root user
USER broker

# Expose broker TCP listen port
EXPOSE 9092

# Healthcheck checking real TCP socket listener on 127.0.0.1:9092
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD bash -c 'cat < /dev/null > /dev/tcp/127.0.0.1/9092' || exit 1

# Entrypoint running real BrokerNodeMain class
ENTRYPOINT ["java", "-cp", "/app/app.jar:/app/lib/*", "com.broker.BrokerNodeMain"]
CMD ["broker-01", "9092", "ap-south-1a"]
