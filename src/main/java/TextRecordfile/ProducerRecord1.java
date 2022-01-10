package TextRecordfile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerRecord1 {

	public static void main(String[] args) throws IOException {

		Properties pr = new Properties();
		pr.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		pr.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		pr.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer kafkaProducer = new KafkaProducer<>(pr);

		// creating a record
		BufferedReader bufferedReader = new BufferedReader(new FileReader("C:\\kafkaconnect\\text.txt"));
		String line = bufferedReader.readLine();
		System.out.print(line);
		while (true) {
			line = bufferedReader.readLine();
			if (line == null) {
			} else {
				
				ProducerRecord<String, String> record = new ProducerRecord("connect-test", line);

				System.out.println(record.value());
				kafkaProducer.send(record);
				kafkaProducer.flush();

			}
		}
	}
}
