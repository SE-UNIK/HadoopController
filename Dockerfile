# Use an official OpenJDK runtime as a parent image
FROM openjdk:17
LABEL authors="john"

# Set the working directory in the container
WORKDIR /app

# Copy the application's JAR file to the container
COPY target/HadoopController-0.0.1-SNAPSHOT.jar /app/HadoopController-0.0.1-SNAPSHOT.jar

# Expose the port the application runs on
EXPOSE 8080

# Run the JAR file
ENTRYPOINT ["java", "-jar", "HadoopController-0.0.1-SNAPSHOT.jar"]
