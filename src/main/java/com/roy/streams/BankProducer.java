package com.roy.streams;

import com.roy.avroexample.Constants;
import example.bank.Deposit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * We're going to produce messages to a Kafka topic that have randomized bank deposits.
 * We should pick evenly from a list of six people and choose a random deposit for each message
 */
public class BankProducer {
    private final List<String> names;
    private final KafkaProducer<String, Deposit> producer;

    public BankProducer() {
        this.names = new ArrayList<String>() {
            {
                add("John");
                add("Sarah");
                add("Edward");
                add("Vincent");
                add("Julia");
                add("Sophia");
            }
        };
        this.producer = createProducer();
    }

    public KafkaProducer<String, Deposit> createProducer() {
        Properties props = new Properties();
        // These are some boilerplate properties
        props.setProperty(Constants.BOOTSTRAP_SERVERS, Constants.KAFKA_HOST);
        props.setProperty(Constants.ACKS_KEY, Constants.ACKS_VAL);
        props.setProperty(Constants.RETRIES_KEY, Constants.RETRIES_VALUE);

        props.setProperty(Constants.KEY_SERIALIZER_KEY, Constants.KEY_SERIALIZER_VALUE);
        props.setProperty(Constants.VALUE_SERIALIZER_KEY, Constants.VALUE_SERIALIZER_VALUE);

        props.setProperty(Constants.SCHEMA_REGISTRY_KEY, Constants.SCHEMA_REGISTRY_HOST);
        return new KafkaProducer<String, Deposit>(props);
    }

    public void sendMessage(String name, int balance, long timestamp) {
       Deposit deposit = new Deposit(name, balance, timestamp);
       ProducerRecord<String, Deposit> record = new ProducerRecord<>(Constants.BANK_INPUT_TOPIC, deposit);

       // send message
       producer.send(record, (recordMetadata, e) -> {
           if (e == null) {
               System.out.println("Message sent");
               System.out.println(recordMetadata.toString());
           } else {
               e.printStackTrace();
           }
       });
    }

    public void streamMessages() {
        try{
            while (true) {
                for (String name : names) {
                    int deposit = new Random().nextInt(30);
                    long timestamp = Instant.now().getEpochSecond();
                    sendMessage(name, deposit, timestamp);
                }
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            flushAndClose();
        }
    }

    public void flushAndClose() {
        producer.flush();
        producer.close();
    }

    public static void main(String[] args) {
        BankProducer producer = new BankProducer();
        producer.streamMessages();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal");
            producer.flushAndClose();
        }));
    }
}
