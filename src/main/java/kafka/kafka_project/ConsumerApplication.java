package kafka.kafka_project;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerApplication {
	private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

	public Consumer<Long, String> createConsumer(String topic) {
		Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
		props.put("enable.auto.commit", "true");
		// props.put("auto.commit.interval.ms", "1000");
		// props.put("session.timeout.ms", "30000");
		// props.put("key.deserializer",
		// "org.apache.kafka.common.serialization.StringDeserializer");
		// props.put("value.deserializer",
		// "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		final Consumer<Long, String> consumer = new KafkaConsumer<Long, String>(props);
		consumer.subscribe(Collections.singletonList(topic));
		return consumer;
	}
	
	public Consumer<Long, String> createConsumer(String topic, String consumerGroupId) {
		Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		props.put("enable.auto.commit", "true");
		// props.put("auto.commit.interval.ms", "1000");
		// props.put("session.timeout.ms", "30000");
		// props.put("key.deserializer",
		// "org.apache.kafka.common.serialization.StringDeserializer");
		// props.put("value.deserializer",
		// "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		final Consumer<Long, String> consumer = new KafkaConsumer<Long, String>(props);
		consumer.subscribe(Collections.singletonList(topic));
		return consumer;
	}
}
