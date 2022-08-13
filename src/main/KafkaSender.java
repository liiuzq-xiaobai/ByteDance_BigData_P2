package main;
import java.io.*;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSender extends Thread{
		String[] words = new String[]{"apple","banana","peach","watermelon","orange","grape","lemon"};
		String topic = "test";
		int count = 3; //一次发送几个单词
	public KafkaSender() {
	}

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

		Random rand1 = new Random();
		//发送10000个单词
//		for(int i=0;i<20;i++) {
//			String[] buffer = new String[count];
//			for (int j = 0; j < count; j++) {
//				buffer[j]=words[rand1.nextInt(words.length)];
//			}
//			String record = arrayToStr(buffer);
//			String record = words[rand1.nextInt(words.length)];
//			char letter = 'A';
//			int nextChar = rand1.nextInt(26);
//			byte[] payload = (i + " " + Character.toString((char)(letter+nextChar)) + " " + new Date().getTime()).getBytes();
			//System.out.println("Send message: " + i + " " + Character.toString((char)(letter+nextChar)) + " " + new Date().getTime());
//			ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(prop.getProperty("kafka.topic.name"),payload);
//			producer.send(new ProducerRecord<>(topic,record));
			//System.out.println("Send message: " + i + " " + Character.toString((char)(letter+nextChar)) + " " + new Date().getTime());
			//int timeBetween = rand2.nextInt(5)+1;
//			try {
//				Thread.sleep(5000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
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

	private String arrayToStr(String[] str){
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < str.length-1; i++) {
			builder.append(str[i]).append(" ");

		}
		builder.append(str[str.length-1]);
		return builder.toString();
	}

	public static void main(String[] args) throws InterruptedException{
		KafkaSender sender = new KafkaSender();
		sender.start();
	}
	
}
