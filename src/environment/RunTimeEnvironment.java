package environment;

import task.*;
//import task.StreamTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import function.MapFunction;
import operator.OneInputStreamOperator;
import operator.StreamOperator;
import operator.StreamReduce;
import record.Tuple2;

/**
 * @author kevin.zeng
 * @description 全局运行环境
 * @create 2022-08-16
 */
public class RunTimeEnvironment extends Thread{

    //全局环境包含所有的运行实例
    private List<StreamTask<?,?>> tasks;
    
    private List<SourceStreamTask> sourceTasks;
    private List<OneInputStreamTask<?,?>> mapTasks;
    private List<OneInputStateStreamTask<?>> reduceTasks;
    private List<SinkStreamTask<?>> sinkTasks;
    
    private OneInputStreamOperator<String, Tuple2<String, Integer>, MapFunction<String, Tuple2<String, Integer>>> mapper= null;
    private List<StreamReduce<Tuple2<?, ?>>> reducerList = new ArrayList<>();

    public RunTimeEnvironment(){
        tasks = new ArrayList<>();
        sourceTasks = new ArrayList<>();
        mapTasks = new ArrayList<>();
        reduceTasks = new ArrayList<>();
        sinkTasks = new ArrayList<>();
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
    		System.out.println("NO TASK CATEGORY");
    	}
    }
    
    public void setMapper(OneInputStreamOperator<String, Tuple2<String, Integer>, MapFunction<String, Tuple2<String, Integer>>> mapper) {
    	this.mapper = mapper;
    }
    
    public void setReducerList(List<StreamReduce<Tuple2<?, ?>>> reducerList) {
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
	            	foundError(sourceTask, "SOURCE");
	            }
	        }catch(Exception e) {
	        	foundError(sourceTask, "SOURCE");
	        }
        }
    }
    
    public void foundError(StreamTask task, String type) {
    	if(type.equals("SOURCE")) {
    		//TODO
    	}else if(type.equals("SINK")) {
    		//TODO
    	}else if(type.equals("MAP")) {
    		OneInputStreamTask<String, Tuple2<String, Integer>> mapTask = new OneInputStreamTask<>();
    		mapTask.setMainOperator(mapper);
    		
    	}else if(type.equals("REDUCE")) {
    		
    	}else {
    		System.out.println("UNKNOWN TYPE.");
    	}
    }
    
    @Override
    public void run(){
        while(true){
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            checkTask();
        }

    }
}
