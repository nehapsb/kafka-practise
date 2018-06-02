package kafka.kafka_project;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerApplication {

	private final static String BOOTSTRAP_SERVERS =
            "localhost:9092,localhost:9093,localhost:9094";
	

	public KafkaProducer<Long, String> createKafkaProducer() {
		Properties properties = new Properties();
		//properties.put("acks", "all");
		//properties.put("retries", 0);

		// Specify buffer size in config
		properties.put("batch.size", 16384);
		// Reduce the no of requests less than 0
		properties.put("linger.ms", 1);

		// The buffer.memory controls the total amount of memory available to
		// the producer for buffering.
//		properties.put("buffer.memory", 33554432);
//		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return new KafkaProducer<Long, String>(properties);
	}
}
