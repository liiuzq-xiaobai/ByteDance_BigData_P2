package environment;

import task.*;
//import task.StreamTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import function.KeySelector;
import function.MapFunction;
import io.BufferPool;
import io.InputChannel;
import io.SinkBufferPool;
import operator.OneInputStreamOperator;
import operator.StreamOperator;
import operator.StreamReduce;
import record.StreamElement;
import record.Tuple2;

/**
 * @author kevin.zeng
 * @description 系统内部的全局运行环境
 * @create 2022-08-16
 */
public class RunTimeEnvironment extends Thread{

    //全局环境包含所有的运行实例
    private List<StreamTask<?,?>> tasks;
    
    private List<SourceStreamTask> sourceTasks;
    private List<OneInputStreamTask<?,?>> mapTasks;
    private List<OneInputStateStreamTask<?>> reduceTasks;
    private List<SinkStreamTask<?>> sinkTasks;
    
    List<InputChannel<StreamElement>> mapChannel;
    List<BufferPool<StreamElement>> mapBuffer;
    List<InputChannel<StreamElement>> reduceChannel;
    List<BufferPool<StreamElement>> reduceBuffer;
    
    KeySelector<StreamElement, String> keySelector1 = null;
    
    List<StreamTask<String, Tuple2<String, Integer>>> mapTaskList = new ArrayList<>();
    List<StreamTask<Tuple2<String, Integer>, Tuple2<String, Integer>>> reduceTaskList = new ArrayList<>();
    
    BufferPool<StreamElement> sourceBuffer;
	List<SinkBufferPool> result;
    BufferPool<StreamElement> sinkBuffer;
    InputChannel<StreamElement> sinkInput;
    
    private OneInputStreamOperator<String, Tuple2<String, Integer>, MapFunction<String, Tuple2<String, Integer>>> mapper= null;
    private List<StreamReduce<Tuple2<String, Integer>>> reducerList = new ArrayList<>();

    public RunTimeEnvironment(){
        tasks = new ArrayList<>();
        sourceTasks = new ArrayList<>();
        mapTasks = new ArrayList<>();
        reduceTasks = new ArrayList<>();
        sinkTasks = new ArrayList<>();
        mapChannel = new ArrayList();
        mapBuffer = new ArrayList();
        reduceChannel = new ArrayList();
        reduceBuffer = new ArrayList();
    }

    public void addTasks(List<StreamTask<?, ?>> tasks) {
        this.tasks.addAll(tasks);
        for(StreamTask task:tasks) {
        	if(task.getTaskCategory().equals("SOURCE")) {
        		this.sourceTasks.add((SourceStreamTask)task);
        	}else if(task.getTaskCategory().equals("MAP")) {
        		this.mapTasks.add((OneInputStreamTask)task);
        	}
        	else if(task.getTaskCategory().equals("REDUCE")) {
        		this.reduceTasks.add((OneInputStateStreamTask)task);
        	}
        	else if(task.getTaskCategory().equals("SINK")) {
        		this.sinkTasks.add((SinkStreamTask)task);
        	}
        }
    }

    public void addTask(StreamTask<?,?> task){
        tasks.add(task);
        if(task.getTaskCategory().equals("SOURCE")) {
    		this.sourceTasks.add((SourceStreamTask)task);
    	}else if(task.getTaskCategory().equals("MAP")) {
    		this.mapTasks.add((OneInputStreamTask)task);
    	}
    	else if(task.getTaskCategory().equals("REDUCE")) {
    		this.reduceTasks.add((OneInputStateStreamTask)task);
    	}
    	else if(task.getTaskCategory().equals("SINK")) {
    		this.sinkTasks.add((SinkStreamTask)task);
    	}else {
//    		System.out.println("NO TASK CATEGORY");
    	}
    }
    
    public void setMapper(OneInputStreamOperator<String, Tuple2<String, Integer>, MapFunction<String, Tuple2<String, Integer>>> mapper) {
    	this.mapper = mapper;
    }
    
    public void setReducerList(List<StreamReduce<Tuple2<String, Integer>>> reducerList) {
    	this.reducerList = reducerList;
    }

    //TODO 检测运行实例是否正常运行
    public void checkTask(){
        for(SourceStreamTask sourceTask : sourceTasks){
            Thread.State state;
	        try {
	            state = sourceTask.getState();
	            System.out.println(sourceTask.getName() + "---"+ state);
	            if(state.equals(Thread.State.TERMINATED)) {
	            	foundError(sourceTask, "SOURCE", 0);
	            }
	        }catch(Exception e) {
	        	foundError(sourceTask, "SOURCE", 0);
	        }
        }
        for(int i=0;i<mapTasks.size();i++){
        	OneInputStreamTask<?,?> mapTask = mapTasks.get(i);
            Thread.State state;
	        try {
	            state = mapTask.getState();
	            System.out.println(mapTask.getName() + "---"+ state);
	            if(state.equals(Thread.State.TERMINATED)) {
	            	foundError(mapTask, "MAP", i);
	            }
	        }catch(Exception e) {
	        	foundError(mapTask, "MAP", i);
	        }
        }
        for(int i=0;i<reduceTasks.size();i++){
        	OneInputStateStreamTask<?> reduceTask = reduceTasks.get(i);
            Thread.State state;
	        try {
	            state = reduceTask.getState();
	            System.out.println(reduceTask.getName() + "---"+ state);
	            if(state.equals(Thread.State.TERMINATED)) {
	            	foundError(reduceTask, "REDUCE", i);
	            }
	        }catch(Exception e) {
	        	foundError(reduceTask, "REDUCE", i);
	        }
        }
        for(SinkStreamTask<?> sinkTask : sinkTasks){
            Thread.State state;
	        try {
	            state = sinkTask.getState();
	            System.out.println(sinkTask.getName() + "---"+ state);
	            if(state.equals(Thread.State.TERMINATED)) {
	            	foundError(sinkTask, "SINK", 0);
	            }
	        }catch(Exception e) {
	        	foundError(sinkTask, "SINK", 0);
	        }
        }
    }
    
    public void foundError(StreamTask task, String type, int index) {
    	 for(StreamTask t:tasks) {
    		 try {
				t.wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	 }
    	if(type.equals("SOURCE")) {
    		//TODO
    		SourceStreamTask consumer = new SourceStreamTask();
            consumer.name("Restored Source");
            consumer.setOutput(sourceBuffer);
            
            
            tasks.remove(task);
            tasks.add(consumer);
            
            sourceTasks.remove(task);
            sourceTasks.add(consumer);
            
    	}else if(type.equals("SINK")) {
    		//TODO
    		SinkStreamTask<Tuple2<String, Integer>> sinkTask = new SinkStreamTask<>(result, reduceTasks.size());
    		sinkTask.setOutput(sinkBuffer);
    		sinkTask.setInput(sinkInput);
    		sinkTask.name("Restored Sink");
    		
    		 tasks.remove(task);
             tasks.add(sinkTask);
             
             sinkTasks.remove(task);
             sinkTasks.add(sinkTask);
    	}else if(type.equals("MAP")) {
    		/*
    		 * OneInputStreamTask<String, Tuple2<String, Integer>> mapTask = new OneInputStreamTask<>();
            mapTask.setMainOperator(mapper);
            //创建task的上游输入channel
            InputChannel<StreamElement> input = new InputChannel<>();
            //channel数据的提供者是sourceBuffer
            input.bindProviderBuffer(Collections.singletonList(sourceBuffer));
            //为channel绑定所属的运行节点
            input.bindExecutionVertex(vertex);
            //输入管道和task绑定
            mapTask.setInput(input);
            //每个task绑定一个输出池
            mapTask.setOutput(mapBuffer.get(i));
            //设置线程名称
            mapTask.name("Map" + i);
            //每个task绑定key选择器，用于分发数据
            mapTask.setKeySelector(keySelector1);
            //放入管道集合
            mapChannel.add(input);
            //放入运行实例集合
            mapTaskList.add(mapTask);
    		 */
    		OneInputStreamTask<String, Tuple2<String, Integer>> mapTask = new OneInputStreamTask<>();
    		mapTask.setMainOperator(mapper);
    		mapTask.setInput(mapChannel.get(index));
    		mapTask.setOutput(mapBuffer.get(index));
    		mapTask.name("Restored Map " + index );
    		mapTask.setKeySelector(keySelector1);
    		
    		mapTaskList.remove(index);
    		mapTaskList.add(index,mapTask);
    		
    		mapTasks.remove(index);
    		mapTasks.add(index,mapTask);
    		
    		tasks.remove(task);
    		tasks.add(mapTask);
    		
    	}else if(type.equals("REDUCE")) {
    		OneInputStateStreamTask<Tuple2<String, Integer>> reduceTask = new OneInputStateStreamTask<>();
    		reduceTask.setMainOperator(reducerList.get(index));
    		reduceTask.setInput(reduceChannel.get(index));
    		reduceTask.setOutput(reduceBuffer.get(index));
    		reduceTask.name("Restored Reduce " + index);
    		
    		reduceTaskList.remove(index);
    		reduceTaskList.add(index,reduceTask);
    		
    		reduceTasks.remove(index);
    		reduceTasks.add(index, reduceTask);
    		
    		tasks.remove(task);
    		tasks.add(reduceTask);
    		
    	}else {
    		System.out.println("UNKNOWN TYPE.");
    	}
    	
    	//reducer恢复keyedstate
    	// for(Reducer r:reducerList){r.restore();}
    	
    	//恢复所有算子
    	// for(StreamTask<?,?> t:tasks){t.notify();}
    	
    	//source恢复offset
    	// sourceList.get(0).setOffset();
    	// sourceList.get(0).startRead();  	
    	
    	// 
    }
    
    /**
	 * @param reduceTaskList the reduceTaskList to set
	 */
	public void setReduceTaskList(List<StreamTask<Tuple2<String, Integer>, Tuple2<String, Integer>>> reduceTaskList) {
		this.reduceTaskList = reduceTaskList;
	}

	/**
	 * @param sourceBuffer the sourceBuffer to set
	 */
	public void setSourceBuffer(BufferPool<StreamElement> sourceBuffer) {
		this.sourceBuffer = sourceBuffer;
	}

	/**
	 * @param result the result to set
	 */
	public void setResult(List<SinkBufferPool> result) {
		this.result = result;
	}

	/**
	 * @param sinkBuffer the sinkBuffer to set
	 */
	public void setSinkBuffer(BufferPool<StreamElement> sinkBuffer) {
		this.sinkBuffer = sinkBuffer;
	}

	/**
	 * @param sinkInput the sinkInput to set
	 */
	public void setSinkInput(InputChannel<StreamElement> sinkInput) {
		this.sinkInput = sinkInput;
	}

	/**
	 * @param tasks the tasks to set
	 */
	public void setTasks(List<StreamTask<?, ?>> tasks) {
		this.tasks = tasks;
	}

	/**
	 * @param sourceTasks the sourceTasks to set
	 */
	public void setSourceTasks(List<SourceStreamTask> sourceTasks) {
		this.sourceTasks = sourceTasks;
	}

	/**
	 * @param mapTasks the mapTasks to set
	 */
	public void setMapTasks(List<OneInputStreamTask<?, ?>> mapTasks) {
		this.mapTasks = mapTasks;
	}

	/**
	 * @param reduceTasks the reduceTasks to set
	 */
	public void setReduceTasks(List<OneInputStateStreamTask<?>> reduceTasks) {
		this.reduceTasks = reduceTasks;
	}

	/**
	 * @param sinkTasks the sinkTasks to set
	 */
	public void setSinkTasks(List<SinkStreamTask<?>> sinkTasks) {
		this.sinkTasks = sinkTasks;
	}

	/**
	 * @param mapChannel the mapChannel to set
	 */
	public void setMapChannel(List<InputChannel<StreamElement>> mapChannel) {
		this.mapChannel = mapChannel;
	}

	/**
	 * @param mapBuffer the mapBuffer to set
	 */
	public void setMapBuffer(List<BufferPool<StreamElement>> mapBuffer) {
		this.mapBuffer = mapBuffer;
	}

	/**
	 * @param reduceChannel the reduceChannel to set
	 */
	public void setReduceChannel(List<InputChannel<StreamElement>> reduceChannel) {
		this.reduceChannel = reduceChannel;
	}

	/**
	 * @param reduceBuffer the reduceBuffer to set
	 */
	public void setReduceBuffer(List<BufferPool<StreamElement>> reduceBuffer) {
		this.reduceBuffer = reduceBuffer;
	}

	/**
	 * @param keySelector1 the keySelector1 to set
	 */
	public void setKeySelector1(KeySelector<StreamElement, String> keySelector1) {
		this.keySelector1 = keySelector1;
	}

	/**
	 * @param mapTaskList the mapTaskList to set
	 */
	public void setMapTaskList(List<StreamTask<String, Tuple2<String, Integer>>> mapTaskList) {
		this.mapTaskList = mapTaskList;
	}

	@Override
    public void run(){
    	int count = 0;
        while(true){
            try {
                TimeUnit.SECONDS.sleep(1);
                count++;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            checkTask();
            if(count==5) {
            	if(sourceTasks.size()!=0) {
            		this.sourceTasks.get(0).sendcheckpointBarrier();
            	}
            	//this.sendcheckpointBarrier();
            }
        }

    }

    public void receiveAck(String name) {
        System.out.println("Env receive ACK: " + name + "***checkpoint finished***");
    }
}
