import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaSender extends Thread{
	Properties prop = new Properties();
	
	public KafkaSender() {
	}
	
	public void run() {
		prop.setProperty("bootstrap.servers", "localhost:9092");
		prop.setProperty("kafka.topic.name", "test");
		KafkaProducer<String, byte[]> producer = 
				new KafkaProducer<String, byte[]>(this.prop, new StringSerializer(), new ByteArraySerializer());
	
		Random rand1 = new Random();
		Random rand2 = new Random();
		for(int i=0;i<1000;i++) {
			char letter = 'A';
			int nextChar = rand1.nextInt(26);
			byte[] payload = (i + " " + Character.toString((char)(letter+nextChar)) + " " + new Date().getTime()).getBytes();
			//System.out.println("Send message: " + i + " " + Character.toString((char)(letter+nextChar)) + " " + new Date().getTime());
			ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(prop.getProperty("kafka.topic.name"),payload);
			producer.send(record);
			//int timeBetween = rand2.nextInt(5)+1;
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		producer.close();
	}
	
	
	public static void main(String[] args) throws InterruptedException{
		KafkaSender sender = new KafkaSender();
		sender.start();
	}
	
}
