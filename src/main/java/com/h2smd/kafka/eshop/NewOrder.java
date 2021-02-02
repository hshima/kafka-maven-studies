package com.h2smd.kafka.eshop;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrder {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
    	var producer = new KafkaProducer<String, String>(properties());
    	var value = "123456,456789,789123"; //"idPedido,idUsuario,idCompra";
    	
//    	var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", value, value); // Same result as following line as signature infers type from param
    	var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);
    	
//    	producer.send(record).get(); //.send(ProducerRecord pr) receives an asynchronous reply (Future.class), but might not retrieve data, so it's required to include a callback
    	producer.send(record, (data, ex) -> { //includes a callback (Observable)
    		if(ex != null) {
    			ex.printStackTrace();
        		return;
    		}
    		System.out.println("Sucesso" + data.topic() + ":::partition " + data.partition() + "//offset " + "/ offset "+ data.offset() + "/ timestamp " + data.timestamp());
    	}).get();
	}

	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		return properties;
	}
}
