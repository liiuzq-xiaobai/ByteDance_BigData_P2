
import java.util.concurrent.LinkedBlockingDeque;

public class Main {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		LinkedBlockingDeque<ObjectWrapper> bq = new LinkedBlockingDeque<ObjectWrapper>();
		KafkaSender sender = new KafkaSender();
		KafkaReceiver receiver = new KafkaReceiver();
		receiver.setQueue(bq);
		Mapper mapper = new Mapper();
		mapper.setQueue(bq);
		sender.start();
		receiver.start();
		mapper.start();
		
	}

}
