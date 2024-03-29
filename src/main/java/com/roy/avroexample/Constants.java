package com.roy.avroexample;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class Constants {
    private Constants() {}

    // Kafka Constants
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String KAFKA_HOST = "127.0.0.1:9092";

    // Producer Constants
    public static final String ACKS_KEY = "acks";
    public static final String ACKS_VAL = "1";
    public static final String RETRIES_KEY = "retries";
    public static final String RETRIES_VALUE = "10";
    public static final String LINGER_MS_VALUE = "1";
    public static final String ENABLE_IDEMPOTENCE_VALUE = "true";

    // Consumer constants
    public static final String GROUP_ID_KEY = "group.id";
    public static final String GROUP_ID_VALUE = "my-avro-consumer";
    public static final String ENABLE_AUTO_COMMIT_KEY = "enable.auto.commit";
    public static final String ENABLE_AUTO_COMMIT_VALUE = "false";
    public static final String AUTO_OFFSET_RESET_KEY = "auto.offset.reset";
    public static final String AUTO_OFFSET_RESET_VALUE = "earliest";
    public static final String SPECIFIC_AVRO_READER_KEY = "specific.avro.reader";
    public static final String SPECIFIC_AVRO_READER_VALUE = "true";

    // Serialization constants
    public static final String KEY_SERIALIZER_KEY = "key.serializer";
    public static final String KEY_DESERIALIZER_KEY = "key.deserializer";
    public static final String STRING_SERIALIZER = StringSerializer.class.getName();
    public static final String STRING_DESERIALIZER = StringDeserializer.class.getName();
    public static final String VALUE_SERIALIZER_KEY = "value.serializer";
    public static final String VALUE_DESERIALIZER_KEY = "value.deserializer";
    public static final String AVRO_SERIALIZER = KafkaAvroSerializer.class.getName();
    public static final String AVRO_DESERIALIZER = KafkaAvroDeserializer.class.getName();

    // Schema registry constants
    public static final String SCHEMA_REGISTRY_KEY = "schema.registry.url";
    public static final String SCHEMA_REGISTRY_HOST = "http://127.0.0.1:8081";

    // Kafka topics
    public static final String TODO_TOPIC = "todo-topic";
    public static final String BANK_INPUT_TOPIC = "bank-input";
    public static final String BANK_OUTPUT_TOPIC = "bank-output";
    public static final String COLOR_INPUT_TOPIC = "color-input";
    public static final String COLOR_OUTPUT_TOPIC = "color-output";

    // Streams config
    public static final String STREAMS_APP_ID = "word-count-application";
    public static final String BANK_APP_ID = "bank-application";
    public static final String STREAMS_INPUT_TOPIC = "word-count-input";
    public static final String STREAMS_OUTPUT_TOPIC = "word-count-output";
}
