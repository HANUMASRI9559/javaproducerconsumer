import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
public class Synchronous {
public static void main(String[] args) throws InterruptedException, ExecutionException {
	
	//CREATE OBJECT FOR PROPERTIES
	Properties pr=new Properties();
	
	//SETTING PROPERTIES
	pr.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
	pr.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	pr.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	
	//CREATE OBJECT FOR PRODUCER
	KafkaProducer kp=new KafkaProducer(pr);
	
	//CREATING PRODUCER RECORD
	ProducerRecord<String, String> record=new ProducerRecord<String, String>("synchronous","retyjh");
	kp.send(record).get();
	kp.flush();
	kp.close();
}
}
