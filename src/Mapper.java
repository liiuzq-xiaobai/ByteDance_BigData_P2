import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Mapper extends Thread{
	BlockingQueue<ObjectWrapper> inputQueue = null;
	BlockingQueue<ObjectWrapper> outputQueue = null;
	HashMap<String, Integer> map = new HashMap<String, Integer>();
	
	public Mapper() {
		
	}
	
	public void setQueue(LinkedBlockingDeque<ObjectWrapper> sharedQueue) {
		this.inputQueue = sharedQueue;
	}
	
	public void run() {
		long currentTime = new Date().getTime();
		while(true) {
			if(inputQueue!=null) {
				try {
					ObjectWrapper obj = inputQueue.take();
					//System.out.println("Mapper: " + obj.getIndex() + " " + obj.getValue() + " " + obj.getTime());
					if(map.keySet().contains(obj.getValue())) {
						map.put(obj.getValue(), map.get(obj.getValue())+1);
					}else {
						map.put(obj.getValue(), 1);
					}
					
					if(new Date().getTime() - currentTime >= 30000) {
						System.out.println("-----------------------");
						Date date = new Date(currentTime);
						SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss");
					    format.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
					    String day = format.format(date);
						for(String key : map.keySet()) {
							System.out.println(day + ", " + key + ", " + map.get(key));
						}
						currentTime = new Date().getTime();
					}
				}catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String[] args) throws InterruptedException{
		Mapper mapper = new Mapper();
		mapper.start();
	}

}
