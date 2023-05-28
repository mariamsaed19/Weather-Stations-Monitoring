# Use official Java 11 image as base image
FROM openjdk:11
FROM maven:3.8.1-jdk-11-slim AS builder

# Set working directory in the container
WORKDIR /app

# Copy the pom.xml file to the container
COPY pom.xml .

# Copy the rest of the source code to the container
COPY src ./src

# Download all the dependencies for the project
RUN mvn dependency:go-offline -B

    
# Build the project
RUN mvn clean install

ENV CLASSPATH=target/lib/*

# Start the application
CMD java -jar target/weather-station.jar
