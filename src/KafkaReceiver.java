import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaReceiver extends Thread{
	static final String TOPIC = "test";
	static final String GROUP = "test_group1";
	
	BlockingQueue<ObjectWrapper> dataQueue = null;
	
	public KafkaReceiver() {
		
	}
	
	public void setQueue(LinkedBlockingDeque<ObjectWrapper> sharedQueue) {
		this.dataQueue = sharedQueue;
	}
	

	public void run() {
		// TODO Auto-generated method stub
		Properties props = new Properties();
		props.put("bootstrap.servers", "120.26.142.199:9092");
		props.put("group.id", GROUP);
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		try(KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);){
			consumer.subscribe(Arrays.asList(TOPIC));
			
			for(int i=0;i<1000;i++) {
				ConsumerRecords<String, String> records = consumer.poll(1000L);
				for (ConsumerRecord<String, String> record:records) {
					String obj = record.value();
					System.out.println("Receive message: " + obj);
					String[] parseObj = obj.split(" ");
					ObjectWrapper dataObj = new ObjectWrapper(Integer.parseInt(parseObj[0]), parseObj[1], Long.parseLong(parseObj[2]));
					if(dataQueue!=null) {
						try {
							dataQueue.put(dataObj);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}

	}
	
	public static void main(String args[]) throws InterruptedException {
		KafkaReceiver receiver = new KafkaReceiver();
		receiver.start();
	}

}
