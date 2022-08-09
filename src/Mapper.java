import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class Mapper extends Thread implements Job{
	BlockingQueue<ObjectWrapper> inputQueue = null;
	BlockingQueue<ObjectWrapper> outputQueue = null;
	
	public Mapper() {
		
	}
	
	public void setInput(LinkedBlockingDeque<ObjectWrapper> sharedQueue) {
		this.inputQueue = sharedQueue;
	}
	
	public void run(){
		ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<String, Integer>();
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
					Thread.sleep(1000);
					
					if(new Date().getTime() - currentTime >= 30000) {
						System.out.println("-----------" + this.getName() + "-----------");
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

	@Override
	public void setOutput(LinkedBlockingDeque<ObjectWrapper> queue) {
		// TODO Auto-generated method stub
		
	}

}
