FROM openjdk:8-slim

WORKDIR /usr/app/src

COPY Consumer_java.java ./

# RUN pip install --no-cache-dir --upgrade pip
# RUN apt-get --no-cache-dir -r requirements.txt
# RUN apt-get update && apt-get -y install org.apache.kafka.clients.consumer.*

# RUN java -cp libs SimpleProducer *topicname*
# RUN javac -cp libs *.java
# RUN apt-get update && apt-get install -y --no-install-recommends libc6 libx11-6 libxext6 libxrender1 libstdc++ libssl1.0 libfreetype6 fontconfig && apt-get clean
RUN ["javac",  "Consumer_java.java"]

ENTRYPOINT ["java","Consumer_java"]