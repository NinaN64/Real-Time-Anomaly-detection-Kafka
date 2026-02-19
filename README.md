## How to run the code?
### Starting Docker/Kafka
docker-compose up -d (starting docker/kafka)

### Creating a topic
docker exec -it kafka-wordcount-kafka-1 \
kafka-topics --create \
--topic wordcount-topic \
--bootstrap-server localhost:9092 \
--replication-factor 1 \
--partitions 1

### Build a project 
mvn clean package

### Run Consumer
mvn exec:java -Dexec.mainClass="com.example.WordCountConsumer"

### Run Producer
mvn exec:java -Dexec.mainClass="com.example.SimpleProducer"


