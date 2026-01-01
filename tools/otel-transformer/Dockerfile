FROM eclipse-temurin:17-jre

WORKDIR /app

# Copy Gradle installDist output from the  module
COPY tools/otel-transformer/build/install/otel-transformer/ /app/

EXPOSE 10001 9464

# Use Gradle application launcher
ENTRYPOINT ["/app/bin/otel-transformer"]
