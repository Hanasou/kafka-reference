package com.roy.avroexample;

public class RunKafkaConsumer {
    public static void main(String[] args) {
        AvroConsumer consumer = new AvroConsumer();

        try {
            consumer.consumeMessages();
        } catch(InterruptedException e) {
            System.out.println("Done");
        } finally {
            consumer.closeConsumer();
        }
    }
}
