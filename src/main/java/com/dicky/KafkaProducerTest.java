package com.dicky;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerTest {

	public static void main(String[] args) {
		Properties props = new Properties();
		 props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		 props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		 props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		 
		 Producer<String, String> producer = new KafkaProducer<String, String>(props);
		 for (int i = 0; i < 100; i++) {
			 System.out.println("sending Data ke " + i);
		     ProducerRecord<String, String> record = new ProducerRecord<String, String>("test-topic", "Data ke " + i);
		     producer.send(record);
		 }
		 
		 producer.close();

	}

}
