package main;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class DataStream {
	int mapCount;
	int reduceCount;
	List<BlockingQueue<ObjectWrapper>> queues = null;
	List<Map<String, ArrayList<String>>> lists = null;
	ConcurrentHashMap<String, String> sink = null;
	boolean checkFlag = false;
	
	public DataStream(int mapCount, int reduceCount) {
		this.mapCount = mapCount;
		this.reduceCount = reduceCount;
		queues = new ArrayList<BlockingQueue<ObjectWrapper>>();
		for(int i=0;i<mapCount;i++) {
			this.queues.add(new LinkedBlockingDeque<ObjectWrapper>());
		}
		lists = new ArrayList<Map<String, ArrayList<String>>>();
		for(int i=0;i<mapCount;i++) {
			this.lists.add(Collections.synchronizedMap(new LinkedHashMap<String, ArrayList<String>>()));
		}
		sink = new ConcurrentHashMap<String, String>();
	}
	
	public void addToQueues(ObjectWrapper obj) {
		if(obj!=null) {
			int index = obj.getKey().hashCode();
			queues.get(index%mapCount).add(obj);
		}
	}
	
	public void addToAllQueues(ObjectWrapper obj) {
		if(obj!=null) {
			for(BlockingQueue<ObjectWrapper> queue : queues) {
				queue.add(obj);
			}
		}
	}
	
	public ObjectWrapper getNext(int index) {
		try {
			return queues.get(index).take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public void addToMaps(ObjectWrapper obj) {
		if(obj!=null) {
			int mapIndex = obj.getKey().hashCode()%reduceCount;
			Map<String, ArrayList<String>> map = lists.get(mapIndex);
			/*
			if(obj.getKey().equals("--CHECKPOINT--")) {
				map.put("--CHECKPOINT--", null);
			}
			*/
			if(map.containsKey(obj.getKey())) {
				map.get(obj.getKey()).add(obj.getValue());
			}else {
				ArrayList<String> values = new ArrayList<String>();
				values.add(obj.getValue());
				map.put(obj.getKey(), values);
			}
		}
	}
	
	public void addToAllMaps(ObjectWrapper obj) {
		if(obj.getKey().equals("--CHECKPOINT--")) {
			for(Map<String, ArrayList<String>> map : lists) {
				if(!map.containsKey("--CHECKPOINT--")) {
					map.put("--CHECKPOINT--", null);
				}
				//map.put("--CHECKPOINT--", null);
			}
		}
	}
	
	public Map<String, ArrayList<String>> getMap(int index){
		return lists.get(index);
	}
	
	public void addToSink(ObjectWrapper obj) {
		if(obj!=null) {
			sink.put(obj.getKey(), obj.getValue());
		}
	}
	
	public ConcurrentHashMap<String, String> getSink() {
		return sink;
	}
	
	public void createCheckpoint() {
		this.checkFlag = true;
		
	}

}
