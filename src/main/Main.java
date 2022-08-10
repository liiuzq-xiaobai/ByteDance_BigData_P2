package main;

import java.util.concurrent.LinkedBlockingDeque;

import customized.MapReduce;
//import customized.Mapper;

public class Main {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		//LinkedBlockingDeque<ObjectWrapper> bq = new LinkedBlockingDeque<ObjectWrapper>();
		Router sourceToMapper = new Router(1, 1);
		KafkaSender sender = new KafkaSender();
		KafkaReceiver receiver = new KafkaReceiver();
		//receiver.setQueue(bq);
		receiver.setRouter(sourceToMapper);
		MapReduce mapper1 = new MapReduce();
		MapReduce reduce1 = new MapReduce();
		//Mapper mapper2 = new Mapper();
		MapJob newMapJob1 = new MapJob(mapper1,0);
		ReduceJob newReduceJob1 = new ReduceJob(reduce1,0);
		Sink sink = new Sink(sourceToMapper);
		//MapJob newJob2 = new MapJob(mapper2,0);
		newMapJob1.jobSetup(sourceToMapper);
		newReduceJob1.jobSetup(sourceToMapper);
		//newJob2.jobSetup(sourceToMapper);
		sender.start();
		receiver.start();
		newMapJob1.start();
		newReduceJob1.start();
		sink.start();
		//Thread.sleep(100);
		//newJob2.start();
		//mapper.start();
		while(true) {
			Thread.sleep(10000);
			sourceToMapper.addToQueues(new ObjectWrapper("--CHECKPOINT--", null));
		}
	}

}
