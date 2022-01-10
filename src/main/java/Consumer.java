import java.*;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

   

public class Consumer {
	public static void main(String[] args) {

		Properties pr = new Properties();

		pr.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		pr.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		pr.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"BookDeserializer");
		pr.put(ConsumerConfig.GROUP_ID_CONFIG, "id3");
		pr.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer consumer = new KafkaConsumer<>(pr);
		
		consumer.subscribe(Collections.singletonList("producer"));
		while(true)
		{
			ConsumerRecords<String,Book> record=consumer.poll(Duration.ofMillis(90));
			for(ConsumerRecord<String,Book> consumerRecord : record)
			{
				Book values =consumerRecord.value();
				System.out.println(values.toString());
				
				//System.out.println("key:" +consumerRecord.key()+ " " +"Value:" +consumerRecord.value()+""+consumerRecord.topic());
			}
			
		}
		

	}

	//public static void subscribe(List<String> singletonList) {
		// TODO Auto-generated method stub
		
	


	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	static ConsumerRecords<String, Book> poll(int i) {
		// TODO Auto-generated method stub
		return null;
	}

	
		
	}

	
		
	


