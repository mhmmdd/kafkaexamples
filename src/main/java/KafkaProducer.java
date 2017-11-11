import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // producer acts
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "3");
        properties.setProperty("linger.ms", "5");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "3", "mesaj 56");
        producer.send(producerRecord);
        producer.close();
    }
}
