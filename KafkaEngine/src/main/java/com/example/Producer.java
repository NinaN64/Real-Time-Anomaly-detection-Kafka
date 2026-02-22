package com.example;

public class Producer {
    private final String bootstrapServers;

    public Producer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        // Initialize Kafka producer
    }

    public void sendMessage(String topic, String message) {
        // send message to Kafka topic
        System.out.println("Sending message: " + message + " to topic: " + topic);
    }

    public void close() {
        // Close Kafka producer
        System.out.println("Closing producer");
    }
}
