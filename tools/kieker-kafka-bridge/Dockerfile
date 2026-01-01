FROM eclipse-temurin:17-jre

WORKDIR /app

COPY build/install/kieker-kafka-bridge/ /app/

EXPOSE 10001

ENTRYPOINT ["/app/bin/kieker-kafka-bridge"]
