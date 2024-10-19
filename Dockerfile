# Use a lightweight OpenJDK 11 runtime image
FROM openjdk:23-ea-1-jdk-slim

# Set the working directory in the container
WORKDIR /app

# Copy the packaged Spring Boot application JAR file into the container
COPY target/similake-0.0.1-SNAPSHOT.jar /app/similake-0.0.1-SNAPSHOT.jar

# Expose the port that the Spring Boot application uses
EXPOSE 8080

# Define the command to run your application when the container starts
CMD ["java", "-jar", "similake-0.0.1-SNAPSHOT.jar"]


# build docker image from Dockerfile
# docker build -t tinumistry/similakem2 .

# run docker image
# docker run -p 8080:8080 tinumistry/similakem2

# push docker image to docker hub
# docker push tinumistry/similakem2