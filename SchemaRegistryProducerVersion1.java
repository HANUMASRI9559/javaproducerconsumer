package com.hanumasri.schemaregistry.version1;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.hanumasri.Employee;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class SchemaRegistryProducerVersion1 {
public static void main(String[] args) {
	
	//configuring Normal Producer
	
	Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    
//    Serializer Should be used as KafkaAvroSerializer
   
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    
//    The Actual Schema Registry Server URL
    properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

    Employee employee=new Employee("Hanumasri","Naramamidi");
    
    KafkaProducer<String, Employee> producer = new KafkaProducer<String, Employee>(properties);

   // String topic = "Employee-schema-registry-topic";

//    Constructing Employee Object with the Generation Avro Model Class 
    
    Employee e = Employee.newBuilder()
           
            
            .setFirstName("Hanumasri")
            .setLastName("Naramamidi")
            .build();

    ProducerRecord<String, Employee> producerRecord = new ProducerRecord<String, Employee>(
            "Employee", employee
    );

    System.out.println(employee);
    
//    Sending the Serialized Data to the Topic
  
    producer.send(producerRecord, new Callback(){
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
                System.out.println(metadata);
            } else {
                exception.printStackTrace();
            }
        }
    });

    producer.flush();
    producer.close();

}

}

