FROM openjdk:latest

WORKDIR /usr/app/src

COPY Consumer_java.java ./
COPY kafka-clients-2.5.0.jar ./
COPY kafka-clients-1.0.1.jar ./
COPY kafka-clients-2.8.1.jar ./

RUN ["javac", "-cp", "kafka-clients-2.5.0.jar",  "Consumer_java.java"]

ENTRYPOINT ["java","Consumer_java"]
