package com.example;

public class ProducerApp {
    public static void main(String[] args) {
        String topicName = "my-topic";
        String bootstrapServers = "localhost:9092";
        
        Producer producer = new Producer(bootstrapServers);
        producer.sendMessage(topicName, "Hello, Kafka!");
        producer.close();
    }
}
