import java.util.concurrent.LinkedBlockingDeque;

public interface Job{
	
	abstract public void setInput(LinkedBlockingDeque<ObjectWrapper> queue);
	
	abstract public void run();
	
	abstract public void setOutput(LinkedBlockingDeque<ObjectWrapper> queue);
	
}
