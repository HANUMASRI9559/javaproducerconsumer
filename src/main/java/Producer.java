import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

	public static void main(String[] args) {
		System.out.println("Hiiiii");
		
		//CREATE OBJECT FOR PROPERTIES
		Properties pr=new Properties();
		
		//SETTING PROPERTIES
	    pr.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
	    pr.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
	    pr.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"BookSerializer");
	   
	    
	    //CREATING OBJECT FOR PRODUCER
	   KafkaProducer KafkaProducer=new KafkaProducer(pr);
	    
	   //CREATING PRODUCER RECORD
	    ProducerRecord<String,Book> record = new ProducerRecord<String,Book>("producer",new Book("kmc","apache"));
	    KafkaProducer.send(record); 
	    System.out.println(record.value());
	    KafkaProducer.flush();
	    KafkaProducer.close();
	     
}
}