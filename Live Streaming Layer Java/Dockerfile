FROM maven:3.8.1-adoptopenjdk-15 AS build-function
RUN mkdir /app
WORKDIR /app

COPY pom.xml .
RUN mvn verify --fail-never
#RUN mvn clean package -Dmaven.main.skip -Dmaven.test.skip && rm -r target

COPY . .
#RUN mvn clean package -Dmaven.test.skip
RUN mvn package

#RUN mvn install -DskipTests
#RUN mvn clean install -DskipTests

FROM openjdk:15-jdk
COPY --from=build-function /app/target/*.jar /app.jar
ENTRYPOINT ["java","-jar","/app.jar"]

# FROM openjdk:latest

# WORKDIR /usr/app/src

# COPY Consumer_java.java ./
# COPY kafka-clients-2.5.0.jar ./
# COPY kafka-clients-1.0.1.jar ./
# COPY kafka-clients-2.8.1.jar ./

# RUN ["javac", "-cp", "kafka-clients-2.5.0.jar",  "Consumer_java.java"]

# ENTRYPOINT ["java","Consumer_java"]
