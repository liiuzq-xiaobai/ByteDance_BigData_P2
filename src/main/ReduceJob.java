package main;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReduceJob extends Thread{
	Job job = null;
	int mapperId;
	Router router = null;
	
	public ReduceJob(Job job, int id) {
		this.job = job;
		this.mapperId = id;
	}
	
	public void jobSetup(Router router) {
		this.router = router;
	}
	
	public void run() {
		//job.run();
		while(true) {
			if(router!=null) {
				Map<String, ArrayList<String>> map = router.getMap(mapperId);
				for(Map.Entry<String, ArrayList<String>> set:map.entrySet()) {
					if(set.getKey().equals("--CHECKPOINT--")) {
						System.out.println("Reducer checkpointed.");
						map.remove("--CHECKPOINT--");
					}else {
					job.reduce(set.getKey(), set.getValue().iterator(), router);
					}
				}
			}
		}
	}

}
