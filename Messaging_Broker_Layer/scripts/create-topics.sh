echo -e 'Creating kafka topics'

# kafka-topics --bootstrap-server localhost:9092 --list

docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh \ --create \ --bootstrap-server  localhost:9092 \ --replication-factor 1 \ --partitions 1 \ --topic th1

docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh \ --create \ --bootstrap-server  localhost:9092 \ --replication-factor 1 \ --partitions 1 \ --topic RAW

docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh \ --create \ --bootstrap-server  localhost:9092 \ --replication-factor 1 \ --partitions 1 \ --topic AGGREGATED

docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh \ --create \ --bootstrap-server  localhost:9092 \ --replication-factor 1 \ --partitions 1 \ --topic e_tot

docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh \ --create \ --bootstrap-server  localhost:9092 \ --replication-factor 1 \ --partitions 1 \ --topic DAILY_RAW

docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh \ --create \ --bootstrap-server  localhost:9092 \ --replication-factor 1 \ --partitions 1 \ --topic AGGREGATED_DIFF