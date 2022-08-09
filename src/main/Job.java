package main;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;

public interface Job{
	
	//abstract public void setInput(LinkedBlockingDeque<ObjectWrapper> queue);
	//abstract public void setInputRouter(Router router);
	
	//abstract public void setMapperId(int MapperId);
	
	abstract public void map(ObjectWrapper obj, Router output);
	
	//abstract public void reduce(Iterator<ObjectWrapper> iterator, Router output);

	abstract void reduce(String key, Iterator<String> iterator, Router output);
	
	//abstract public void setOutput(LinkedBlockingDeque<ObjectWrapper> queue);
	
}
