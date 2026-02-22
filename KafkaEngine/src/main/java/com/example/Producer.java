package com.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
    private final KafkaProducer<String, String> producer;

    public Producer(String brokers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        this.producer = new KafkaProducer<>(props);
    }

    public void sendMessageToTopic(String topic, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.printf("Sent to topic %s, partition %d, offset %d%n", 
                                  metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                exception.printStackTrace();
            }
        });
        System.out.println("Sending message: " + message + " to topic: " + topic);
    }

    public void close() {
        producer.close();
        System.out.println("Closing producer");
    }
}
