package com.roy.streams;

import com.roy.avroexample.Constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Locale;
import java.util.Properties;

public class FavoriteColor {
    // Take a comma delimited topic of userid,color
    // Filter out bad data
    // Keep only color of "green", "red", or "blue"
    // Get the running count of the favorite colors overall and output this to a topic
    // A users' favorite color can change
   public static void startFavoriteColor() {
       Properties props = new Properties();
       props.put(StreamsConfig.APPLICATION_ID_CONFIG, Constants.STREAMS_APP_ID);
       props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_HOST);
       props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.AUTO_OFFSET_RESET_VALUE);
       props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
       props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

       // Start with StreamsBuilder
       StreamsBuilder builder = new StreamsBuilder();
       // We probably want a table in this case right?
       KTable<String, String> input = builder.table(Constants.COLOR_INPUT_TOPIC);
       // Transformations
       KTable<String, Long> colorCounts = input.mapValues((value) -> value.toLowerCase())
               .filter((key, value) -> value.equals("red") || value.equals("blue") || value.equals("green"))
               .groupBy((key, value) -> KeyValue.pair(value, 1))
               .count();

       colorCounts.toStream().to(Constants.COLOR_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

       // Build the stream
       KafkaStreams streams = new KafkaStreams(builder.build(), props);
       // Start the stream
       streams.start();
       System.out.println(streams.toString());

       // shutdown hook
       Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
   }
}
