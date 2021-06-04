package com.roy.streams;

import com.roy.avroexample.Constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {

    public static void startWordCount() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Constants.STREAMS_APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_HOST);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.AUTO_OFFSET_RESET_VALUE);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // To build a stream, start with a builder
        StreamsBuilder builder = new StreamsBuilder();
        // Call stream from the builder and pass in the topic you want to get data from
        KStream<String, String> input = builder.stream(Constants.STREAMS_INPUT_TOPIC);
        // Map the values to lowercase
        // We're using a method reference here. Not sure if it's a good idea.
        KTable<String, Long> wordCounts = input.mapValues((ValueMapper<String, String>) String::toLowerCase)
            // FlatMap the value into a list of words
            .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
            // SelectKey to give all our values a new key
            // Make the key the same as the word
            //.selectKey((ignoredKey, word) -> word)
            // Group by word
            .groupBy((key, word) -> word)
            .count(Named.as("count-store"));

        // Finally stream this back to Kafka
        // Provide the topic you want to stream this to
        // Use a produced object to denote your Serdes, because the Serdes that we're using to
        // write to our new topic are different from the defaults we put into props.
        wordCounts.toStream().to(Constants.STREAMS_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        // Build the stream
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        // Start the stream
        streams.start();
        System.out.println(streams.toString());

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static void main(String[] args) {
        startWordCount();
    }
}
