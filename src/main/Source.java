package main;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Source extends Thread{
	static final String TOPIC = "test";
	static final String GROUP = "test_group1";
	
	//BlockingQueue<ObjectWrapper> dataQueue = null;
	DataStream router = null;
	
	Random rand = new Random();
	
	public Source() {
		
	}
	
	/**
	public void setQueue(LinkedBlockingDeque<ObjectWrapper> sharedQueue) {
		this.dataQueue = sharedQueue;
	}
	*/
	
	public void setRouter(DataStream router) {
		this.router = router;
	}
	

	public void run() {
		// TODO Auto-generated method stub
		Properties props = new Properties();
		System.out.println("Receive message");
		props.put("bootstrap.servers", "120.26.142.199:9092");
		props.put("group.id", GROUP);
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		try(KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);){
			consumer.subscribe(Arrays.asList(TOPIC));
			
			while(true){
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
				for (ConsumerRecord<String, String> record:records) {
					String obj = record.value();
					//System.out.println("Receive message: " + obj);
					String[] parseObj = obj.split(" ");
					int next = rand.nextInt(3) + 1;
					String randInteger = Integer.toString(next);
					ObjectWrapper dataObj = new ObjectWrapper(parseObj[1], randInteger, Long.parseLong(parseObj[2]));
					//System.out.println("Redirect message: " + parseObj[1] + " " + randInteger);
					if(router!=null) {
						router.addToQueues(dataObj);
					}
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}

	}
	
	/*
	public static void main(String args[]) throws InterruptedException {
		Source receiver = new Source();
		receiver.start();
	}
	*/

}
