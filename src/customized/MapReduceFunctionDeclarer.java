package customized;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import main.*;

public class MapReduceFunctionDeclarer extends Thread implements Job{
	//
	
	//BlockingQueue<ObjectWrapper> inputQueue = null;
	//BlockingQueue<ObjectWrapper> outputQueue = null;
	
	//Router inputRouter = null;
	
	
	public MapReduceFunctionDeclarer() {
		//this.mapperId = mapperId;
	}
	
	/*
	public void setInput(LinkedBlockingDeque<ObjectWrapper> sharedQueue) {
		this.inputQueue = sharedQueue;
	}
	
	public void setOutput(LinkedBlockingDeque<ObjectWrapper> sharedQueue) {
		//this.outputQueue = sharedQueue;
	}
	*/
	/*
	public void setInputRouter(Router router) {
		this.inputRouter = router;
	}
	*/
	
	/*
	public void setMapperId(int mapperId) {
		this.mapperId = mapperId;
	}
	*/
	
	public void map(ObjectWrapper obj, DataStream output){
		output.addToMaps(obj);
		/*
		ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<String, Integer>();
		long currentTime = new Date().getTime();
		//while(true) {
			//if(inputRouter!=null) {
				try {
					//ObjectWrapper obj = inputRouter.getNext(mapperId);
					//System.out.println("Mapper: " +mapperId +" get " + obj.getKey() + " " + obj.getValue() + " " + obj.getTime());
					if(map.keySet().contains(obj.getKey())) {
						map.put(obj.getKey(), map.get(obj.getKey())+Integer.parseInt(obj.getValue()));
					}else {
						map.put(obj.getKey(), Integer.parseInt(obj.getValue()));
					}
					//Thread.sleep(1000);
					
					if(new Date().getTime() - currentTime >= 10000) {
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
				}catch (Exception e) {
					e.printStackTrace();
				}
			//}
		//}
		 * 
		 */
	}
	
	/*
	public static void main(String[] args) throws InterruptedException{
		MapReduce mapper = new MapReduce();
		mapper.start();
	}
	*/

	@Override
	public void reduce(String key, Iterator<String> iterator, DataStream output) {
		// TODO Auto-generated method stub
		int sum = 0;
        while(iterator.hasNext()) {
            sum += Integer.parseInt(iterator.next());
        }
        output.addToSink(new ObjectWrapper(key, Integer.toString(sum)));
	}

}
