package main;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
		//1.创建消费者配置类
		Properties props = new Properties();

		//2.给配置信息赋值
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"120.26.142.199:9092");

		//开启offset自动提交，不提交会重复消费
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
//		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

		//向kafka提交offset的延迟
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");

		//key,value的反序列化
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

		//消费者组
//		props.put(ConsumerConfig.GROUP_ID_CONFIG,"bigdata1");
		System.out.println("Receive message");
//		props.put("group.id", GROUP);
//		props.put("auto.commit.interval.ms", "1000");
//		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		try(KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);){
			consumer.subscribe(Collections.singletonList(TOPIC));
			
			while(true){
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
				for (ConsumerRecord<String, String> record:records) {
					String obj = record.value();
					//放入下游的Buffer中

					System.out.println(obj);
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}

	}
	

	public static void main(String args[]) throws InterruptedException {
		Source receiver = new Source();
		receiver.start();
	}


}
