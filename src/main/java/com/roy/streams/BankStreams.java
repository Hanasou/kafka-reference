package com.roy.streams;

import com.roy.avroexample.Constants;
import example.bank.Deposit;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class BankStreams {

    public static void startStream() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Constants.STREAMS_APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_HOST);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.AUTO_OFFSET_RESET_VALUE);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        // Add schema registry url here too
        props.put(Constants.SCHEMA_REGISTRY_KEY, Constants.SCHEMA_REGISTRY_HOST);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Deposit> input = builder.stream(Constants.BANK_INPUT_TOPIC);
        KTable<String, Deposit> balances = input.selectKey((key, deposit) -> deposit.getFirstName()) // change key to persons' first name
                .groupByKey()
                .reduce((aggValue, newValue) -> {
                    int newAmount = aggValue.getAmount() + newValue.getAmount();
                    return new Deposit(aggValue.getFirstName(), newAmount, newValue.getTimestamp());
                });

        balances.toStream().to(Constants.BANK_OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        System.out.println("Stream: " + streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static void main(String[] args) {
        startStream();
    }
}
