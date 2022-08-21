package main;
import java.io.*;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSender extends Thread{
		String[] words = new String[]{"apple","banana","peach","watermelon","orange","grape","lemon"};
		String topic = "zbw_test2";

	@Override
	public void run() {
		//1.创建Kafka生产者的配置信息
		Properties props = new Properties();
		//2.指定连接的kafka 集群，broker-list
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "120.26.142.199:9092");
		//3.ACK应答级别
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		//4.重试次数
//		props.put("retries", 1);
		//5.批次大小 一次发送多少大小的数据
//		props.put("batch.size", 16384);
		//6.等待时间 1ms后发送
//		props.put("linger.ms", 1);
		//7.RecordAccumulator 缓冲区大小 32M
		props.put("buffer.memory", 33554432);
		//8.key,value序列化类
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer =
				new KafkaProducer<>(props);

		//发送固定的几行单词，用于测试程序正确性
		BufferedReader reader = null;
		try {
			 reader = new BufferedReader(new FileReader("input/words.txt"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		String line = "";
		try {
			line = reader.readLine();
			while (line != null){
				producer.send(new ProducerRecord<>(topic,line));
				line = reader.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		producer.close();
	}

	public static void main(String[] args) {
		KafkaSender sender = new KafkaSender();
		sender.start();
	}
	
}
