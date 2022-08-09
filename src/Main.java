
import java.util.concurrent.LinkedBlockingDeque;

public class Main {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		LinkedBlockingDeque<ObjectWrapper> bq = new LinkedBlockingDeque<ObjectWrapper>();
		KafkaSender sender = new KafkaSender();
		KafkaReceiver receiver = new KafkaReceiver();
		receiver.setQueue(bq);
		Mapper mapper1 = new Mapper();
		Mapper mapper2 = new Mapper();
		MapJob newJob1 = new MapJob(mapper1);
		MapJob newJob2 = new MapJob(mapper2);
		newJob1.jobSetup(bq);
		newJob2.jobSetup(bq);
		sender.start();
		receiver.start();
		newJob1.start();
		Thread.sleep(1000);
		newJob2.start();
		//mapper.start();
		
	}

}
