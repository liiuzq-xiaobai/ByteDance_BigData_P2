package main;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

public class Sink extends Thread{
	DataStream router;
	
	public Sink(DataStream router) {
		this.router = router;
	}
	
	public void run() {
		while(true) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//for (Map.Entry<String, String> set : router.getSink().entrySet()) {
			ConcurrentHashMap<String, String> map = router.getSink();
				String output = "-----------" + this.getName() + "-----------\n";
				Date date = new Date();
				SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss");
			    format.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
			    String day = format.format(date);
				for(String key : map.keySet()) {
					output += day + ", " + key + ", " + map.get(key) + "\n";
				}
				output += "------------------------------";
				System.out.println(output);
			//}
		}
	}
}
