package com.h2smd.kafka.eshop;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class FraudDetectorService {
	public static void main(String[] args) {

		while (true) {

			var consumer = new KafkaConsumer<String, String>(props());
			consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
			var records = consumer.poll(Duration.ofMinutes(1));
			if (records.isEmpty()) {
				System.out.println("Registro não encontrado");
				continue;
			}
			for (var record : records) {
				System.out.println("Processing new order, checking for fraud");
				System.out.println(record.key());
				System.out.println(record.value());
				System.out.println(record.partition());
				System.out.println(record.offset());

				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				System.out.println("Order processed successfully");
			}
		}
	}

	private static Properties props() {
		var props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
		return props;
	}
}
