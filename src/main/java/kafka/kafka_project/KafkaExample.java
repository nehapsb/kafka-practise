package kafka.kafka_project;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaExample {

	private final static String TOPIC = "my-example-topic";

	public static void main(String[] args) {

		Thread producerThread = new Thread(producer);
		Thread consumerThread = new Thread(consumer);
		producerThread.start();
		consumerThread.start();
	}

	static Runnable producer = new Runnable() {

		@Override
		public void run() {
			ProducerApplication producerApplication = new ProducerApplication();
			KafkaProducer<Long, String> kafkaProducer = producerApplication.createKafkaProducer();
			long time = System.currentTimeMillis();
			try {
				for (long i = time; i < time + 25; i++) {
					final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC, i, "Hello" + i);

					RecordMetadata metadata = kafkaProducer.send(record).get();

					long elapsedTime = System.currentTimeMillis() - time;
					System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
							record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
					System.out.println("Message sent successfully");
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			finally {
				kafkaProducer.close();
			}
		}
	};

	static Runnable consumer = new Runnable() {

		@Override
		public void run() {
			ConsumerApplication consumerApplication = new ConsumerApplication();
			Consumer<Long, String> consumer = consumerApplication.createConsumer(TOPIC);
			// consumer.subscribe(TOPIC);

			while (true) {
				final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

				if (consumerRecords.count() == 0) {
					break;
				}

				consumerRecords.forEach(record -> {
					System.out.println("thread " + Thread.currentThread().getName());
					System.out.println(
							"Got Record: (" + record.key() + ", " + record.value() + ")  partition : " + record.partition() + "at offset " + record.offset());
				});
				consumer.commitAsync();
			}
			consumer.close();
			System.out.println("DONE");

		}
	};
	static void runProducer(final int messgaeCount) {

		ProducerApplication producerApplication = new ProducerApplication();
		KafkaProducer<Long, String> kafkaProducer = producerApplication.createKafkaProducer();
		long time = System.currentTimeMillis();

		for (long i = time; i < time + messgaeCount; i++)
			kafkaProducer.send(new ProducerRecord<Long, String>(TOPIC, i, "Hello" + i));
		System.out.println("Message sent successfully");
		kafkaProducer.close();
	}

	static void runConsumer() {
		ConsumerApplication consumerApplication = new ConsumerApplication();
		Consumer<Long, String> consumer = consumerApplication.createConsumer(TOPIC);
		// consumer.subscribe(TOPIC);

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(100);

			if (consumerRecords.count() == 0) {
				break;
			}

			consumerRecords.forEach(record -> {
				System.out.println("Got Record: ( key = " + record.key() + ", value = " + record.value()
						+ ") at offset " + record.offset());
			});
			consumer.commitAsync();
		}
		consumer.close();
		System.out.println("DONE");

	}
}
