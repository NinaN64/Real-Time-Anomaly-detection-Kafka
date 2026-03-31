# 1. Start Kafka
docker-compose up -d

# 2. Create topic (first time only)
docker exec -it real-time-anomaly-detection-kafka-kafka-1 \
  kafka-topics --create \
  --topic raw-documents \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 6

# 3. Export dataset (first time only)
cd KafkaEngine
python3 export_dataset.py

# 4. Build and run
mvn clean package
java -jar target/kafka-engine-1.0-SNAPSHOT.jar

# 5. Verify (separate terminal)
docker exec -it real-time-anomaly-detection-kafka-kafka-1 \
  kafka-console-consumer \
  --topic raw-documents \
  --bootstrap-server localhost:9092 \
  --from-beginning



  cd /Users/ninoczka/Documents/Master/Kafka/Real-Time-Anomaly-detection-Kafka
  docker compose up -d

  cd /Users/ninoczka/Documents/Master/Kafka/Real-Time-Anomaly-detection-Kafka/KafkaEngine
  java -jar target/kafka-engine-1.0-SNAPSHOT.jar

  cd /Users/ninoczka/Documents/Master/Kafka/Real-Time-Anomaly-detection-Kafka/KafkaStreams
  java -jar target/kafka-streams-engine-1.0-SNAPSHOT.jar

  cd /Users/ninoczka/Documents/Master/Kafka/Real-Time-Anomaly-detection-Kafka/AnomalyDetection
  python3 ConsumerApp.py
