package com.roy.streams;

import com.roy.avroexample.Constants;
import example.bank.Deposit;
import example.todo.ToDo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class BankConsumer {
    private final KafkaConsumer<String, Deposit> consumer;

    public BankConsumer() {
        this.consumer = createConsumer();
    }

    public KafkaConsumer<String, Deposit> createConsumer() {
        Properties props = new Properties();
        // These are some boilerplate properties
        props.setProperty(Constants.BOOTSTRAP_SERVERS, Constants.KAFKA_HOST);
        props.setProperty(Constants.GROUP_ID_KEY, Constants.GROUP_ID_VALUE);
        props.setProperty(Constants.ENABLE_AUTO_COMMIT_KEY, Constants.ENABLE_AUTO_COMMIT_VALUE);
        props.setProperty(Constants.AUTO_OFFSET_RESET_KEY, Constants.AUTO_OFFSET_RESET_VALUE);

        props.setProperty(Constants.KEY_DESERIALIZER_KEY, Constants.KEY_DESERIALIZER_VALUE);
        props.setProperty(Constants.VALUE_DESERIALIZER_KEY, Constants.VALUE_DESERIALIZER_VALUE);

        props.setProperty(Constants.SCHEMA_REGISTRY_KEY, Constants.SCHEMA_REGISTRY_HOST);
        // Indicate that we want to read specific Avro records
        props.setProperty(Constants.SPECIFIC_AVRO_READER_KEY, Constants.SPECIFIC_AVRO_READER_VALUE);

        return new KafkaConsumer<String, Deposit>(props);
    }

    public void consumeMessages(String topic) {
        consumer.subscribe(Collections.singleton(topic));

        // Continuously poll for more data
        while (true) {
            ConsumerRecords<String, Deposit> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, Deposit> record : records) {
                Deposit deposit = record.value();
                System.out.println(deposit);
            }
            // Commit offsets
            consumer.commitSync();
        }
    }

    public void closeConsumer() {
        consumer.close();
    }

    public static void main(String[] args) {
        BankConsumer consumer = new BankConsumer();
        consumer.consumeMessages(Constants.BANK_OUTPUT_TOPIC);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal");
            consumer.closeConsumer();
        }));
    }
}
