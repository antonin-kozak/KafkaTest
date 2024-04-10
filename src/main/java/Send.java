import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Send {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping producer");
            producer.close();
        }));

        for (int i = 0; i < 100; i++) {
            String topic = AdminHelper.TOPIC_NAME;
            String key = Integer.toString(i);
            String value = Integer.toString(i * 2);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record);
            System.out.println("Sent: " + i);
        }
        producer.close();

    }
}
