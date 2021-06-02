package com.roy.avroexample;

import example.todo.ToDo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AvroConsumer {
    private KafkaConsumer<String, ToDo> consumer;

    public AvroConsumer() {
        this.consumer = this.createConsumer();
    }

    public KafkaConsumer<String, ToDo> createConsumer() {
        Properties props = new Properties();
        // These are some boilerplate properties
        props.setProperty(Constants.BOOTSTRAP_SERVERS, Constants.KAFKA_HOST);
        props.setProperty(Constants.GROUP_ID_KEY, Constants.GROUP_ID_VALUE);
        props.setProperty(Constants.ENABLE_AUTO_COMMIT_KEY, Constants.ENABLE_AUTO_COMMIT_VALUE);
        props.setProperty(Constants.AUTO_OFFSET_RESET_KEY, Constants.AUTO_OFFSET_RESET_VALUE);

        props.setProperty(Constants.KEY_SERIALIZER_KEY, Constants.KEY_SERIALIZER_VALUE);
        props.setProperty(Constants.VALUE_SERIALIZER_KEY, Constants.VALUE_SERIALIZER_VALUE);

        props.setProperty(Constants.SCHEMA_REGISTRY_KEY, Constants.SCHEMA_REGISTRY_HOST);
        // Indicate that we want to read specific Avro records
        props.setProperty(Constants.SPECIFIC_AVRO_READER_KEY, Constants.SPECIFIC_AVRO_READER_VALUE);

        return new KafkaConsumer<String, ToDo>(props);
    }

    public void consumeMessages() throws InterruptedException {
        consumer.subscribe(Collections.singleton(Constants.TODO_TOPIC));

        // Continuously poll for more data
        while (true) {
            ConsumerRecords<String, ToDo> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, ToDo> record : records) {
                ToDo todo = record.value();
                System.out.println(todo);
            }
            // Commit offsets
            consumer.commitSync();
        }
    }

    public void closeConsumer() {
        consumer.close();
    }
}
