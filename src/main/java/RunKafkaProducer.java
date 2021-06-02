import com.roy.avroexample.AvroConsumer;
import com.roy.avroexample.AvroProducer;

import java.time.Instant;

public class RunKafkaProducer {
    public static void main(String[] args) {
        AvroProducer producer = new AvroProducer();

        String taskName = "Finish poc";
        String assignee = "Roy";
        int duration = 50;
        long timestamp = Instant.now().getEpochSecond();
        producer.produceMessage(taskName, assignee, duration, timestamp);

        producer.flushAndClose();
    }
}
