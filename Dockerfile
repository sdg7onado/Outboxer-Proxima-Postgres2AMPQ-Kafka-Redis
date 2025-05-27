#-------------------------------------------------------#
# First stage: image to build the application           #
#-------------------------------------------------------#
# The gradle version here should match the gradle version in gradle/wrapper/gradle-wrapper.properties
FROM eclipse-temurin:24-jdk-alpine AS builder

WORKDIR /builder

# Install Gradle
RUN apk add --no-cache curl unzip && \
	curl -fsSL https://services.gradle.org/distributions/gradle-8.4-bin.zip -o gradle.zip && \
	unzip gradle.zip -d /opt && \
	ln -s /opt/gradle-8.4/bin/gradle /usr/bin/gradle && \
	rm gradle.zip

# Copy project files
COPY *.gradle /builder/
COPY src /builder/src

# Build the project
RUN gradle installDist

#-------------------------------------------------------#
# Second stage: image to run the application            #
#-------------------------------------------------------#
FROM adoptopenjdk/openjdk15-openj9:alpine-slim

# Add some OCI container image labels
# See https://github.com/opencontainers/image-spec/blob/master/annotations.md#pre-defined-annotation-keys
LABEL org.opencontainers.image.source="https://github.com/jvalue/outboxer-postgres2rabbitmq"
LABEL org.opencontainers.image.licenses="MIT"

RUN mkdir /app
WORKDIR /app

# Pull the dist files from the builder container
COPY --from=builder /builder/build/install/ .

# Run app
ENTRYPOINT ["./outboxer-postgres2rabbitmq/bin/outboxer-postgres2rabbitmq"]
