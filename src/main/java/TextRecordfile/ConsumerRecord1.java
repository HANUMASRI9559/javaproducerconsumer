package TextRecordfile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerRecord1 {
	public static void main(String[] args) {

		Properties pr = new Properties();
		pr.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		pr.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		pr.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		pr.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "languagegrpp");
		pr.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

		KafkaConsumer consumer = new KafkaConsumer(pr);
		consumer.subscribe(Arrays.asList("connect-test"));

		try {
			File file = new File("C:\\kafkaconnect\\text1.txt");
			FileOutputStream fileoutputstream = new FileOutputStream(file);
			BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileoutputstream));

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(90));
				for (ConsumerRecord<String, String> record : records) {
					 {
						bufferedWriter.write(record.value());
						System.out.println(record.value().toString());
						bufferedWriter.newLine();
						//bufferedWriter.flush();
					} 
						//throw new FileNotFoundException();
					
				}
			}
		} catch (Exception e) {

			e.printStackTrace();

		}
	}
}
