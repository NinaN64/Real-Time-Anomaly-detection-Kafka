package com.example;

public class ProducerApp {
    public static void main(String[] args) {
        String topicName = "topic";
        String broker = "localhost:9092";
        
        Producer producer = new Producer(broker);
        producer.sendMessageToTopic(topicName, "Hello, Kafka!");
        producer.close();
    }
}
