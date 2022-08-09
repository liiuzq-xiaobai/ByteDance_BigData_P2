import java.util.concurrent.LinkedBlockingDeque;

public class MapJob extends Thread{
	Job job = null;
	
	public MapJob(Job job) {
		this.job = job;
	}
	
	public void jobSetup(LinkedBlockingDeque<ObjectWrapper> list) {
		job.setInput(list);
	}
	
	public void run() {
		job.run();
	}

}
