package com.roy.avroexample;

import example.todo.ToDo;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class AvroProducer {
    private KafkaProducer<String, ToDo> producer;

    public AvroProducer() {
        this.producer = this.createProducer();
    }

    public KafkaProducer<String, ToDo> createProducer() {
        Properties props = new Properties();
        // These are some boilerplate properties
        props.setProperty(Constants.BOOTSTRAP_SERVERS, Constants.KAFKA_HOST);
        props.setProperty(Constants.ACKS_KEY, Constants.ACKS_VAL);
        props.setProperty(Constants.RETRIES_KEY, Constants.RETRIES_VALUE);

        props.setProperty(Constants.KEY_SERIALIZER_KEY, Constants.KEY_SERIALIZER_VALUE);
        props.setProperty(Constants.VALUE_SERIALIZER_KEY, Constants.VALUE_SERIALIZER_VALUE);

        props.setProperty(Constants.SCHEMA_REGISTRY_KEY, Constants.SCHEMA_REGISTRY_HOST);

        // Initialize KafkaProducer
        KafkaProducer<String, ToDo> producer = new KafkaProducer<String, ToDo>(props);
        return producer;
    }

    public void produceMessage(String task, String assignee, int duration, long timestamp) {
        ToDo todo = new ToDo(task, assignee, duration, timestamp);

        ProducerRecord<String, ToDo> record = new ProducerRecord<String, ToDo>(Constants.TODO_TOPIC, todo);

        // send message
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    System.out.println("Message Sent");
                    System.out.println(recordMetadata.toString());
                } else {
                    e.printStackTrace();
                }
            }
        });
    }

    public void flushAndClose() {
        producer.flush();
        producer.close();
    }
}
