package main;

import java.util.concurrent.LinkedBlockingDeque;

import customized.MapReduceFunctionDeclarer;
//import customized.Mapper;

public class Main {

	public static void main(String[] args) throws InterruptedException {
		//LinkedBlockingDeque<ObjectWrapper> bq = new LinkedBlockingDeque<ObjectWrapper>();
		DataStream sourceToMapper = new DataStream(1, 1);
		KafkaSender sender = new KafkaSender();
		Source receiver = new Source();
		//output.setQueue(bq);
		receiver.setRouter(sourceToMapper);
		MapReduceFunctionDeclarer mapper1 = new MapReduceFunctionDeclarer();
		MapReduceFunctionDeclarer reduce1 = new MapReduceFunctionDeclarer();
		//Mapper mapper2 = new Mapper();
		MapOperator newMapJob1 = new MapOperator(mapper1,0);
		ReduceOperator newReduceJob1 = new ReduceOperator(reduce1,0);
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
