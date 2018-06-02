package kafka.kafka_project;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaApplicationMultipleConsumerGruop {



	private final static String TOPIC = "my-example-topic";

	public static void main(String[] args) {

		Thread producerThread = new Thread(producer);
		Thread consumerThread = new Thread(consumer1);
		consumerThread.setName("consumerThread1");
		producerThread.start();
		consumerThread.start();
		Thread consumerThread1 = new Thread(consumer2);
		consumerThread1.setName("consumerThread2");
		Thread consumerThread2 = new Thread(consumer3);
		consumerThread2.setName("consumerThread3");
		consumerThread1.start();
		consumerThread2.start();
	}

	static Runnable producer = new Runnable() {

		@Override
		public void run() {
			ProducerApplication producerApplication = new ProducerApplication();
			KafkaProducer<Long, String> kafkaProducer = producerApplication.createKafkaProducer();
			long time = System.currentTimeMillis();
			try {
				for (long i = time; i < time + 5; i++) {
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

	static Runnable consumer1 = new Runnable() {

		@Override
		public void run() {
			ConsumerApplication consumerApplication = new ConsumerApplication();
			Consumer<Long, String> consumer = consumerApplication.createConsumer(TOPIC, "KafkaConsumer1");
			// consumer.subscribe(TOPIC);

			while (true) {
				final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

				if (consumerRecords.count() == 0) {
					break;
				}

				consumerRecords.forEach(record -> {
					//System.out.println("thread " + Thread.currentThread().getName());
					System.out.println("Consumer 1 -->" +
							"Got Record: (" + record.key() + ", " + record.value() + ")  partition : " + record.partition() + " at offset " + record.offset());
				});
				consumer.commitAsync();
			}
			consumer.close();
			System.out.println("DONE");

		}
	};
	static Runnable consumer2 = new Runnable() {

		@Override
		public void run() {
			ConsumerApplication consumerApplication = new ConsumerApplication();
			Consumer<Long, String> consumer = consumerApplication.createConsumer(TOPIC, "KafkaConsumer2");
			// consumer.subscribe(TOPIC);

			while (true) {
				//System.out.println("thread " + Thread.currentThread().getName());
				final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

				if (consumerRecords.count() == 0) {
					break;
				}

				consumerRecords.forEach(record -> {
					System.out.println("Consumer 2 -->" +
							"Got Record: (" + record.key() + ", " + record.value() + ") partition : " + record.partition() + " at offset " + record.offset());
				});
				consumer.commitAsync();
			}
			consumer.close();
			System.out.println("DONE");

		}
	};
	static Runnable consumer3 = new Runnable() {

		@Override
		public void run() {
			ConsumerApplication consumerApplication = new ConsumerApplication();
			Consumer<Long, String> consumer = consumerApplication.createConsumer(TOPIC, "KafkaConsumer3");
			// consumer.subscribe(TOPIC);

			while (true) {
				final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

				if (consumerRecords.count() == 0) {
					break;
				}
				//System.out.println("thread " + Thread.currentThread().getName());
				consumerRecords.forEach(record -> {
					System.out.println("Consumer 3 -->" +
							"Got Record: (" + record.key() + ", " + record.value() + ") partition : " + record.partition() + " at offset " + record.offset());
				});
				consumer.commitAsync();
			}
			consumer.close();
			System.out.println("DONE");

		}
	};


}
