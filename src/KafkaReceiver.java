import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaReceiver {
	static final String TOPIC = "test";
	static final String GROUP = "test_group1";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", GROUP);
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		try(KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);){
			consumer.subscribe(Arrays.asList(TOPIC));
			
			for(int i=0;i<1000;i++) {
				ConsumerRecords<String, String> records = consumer.poll(1000L);
				for (ConsumerRecord<String, String> record:records) {
					System.out.println("Receive message: " + record.value());
				}
			}
		}

	}

}
