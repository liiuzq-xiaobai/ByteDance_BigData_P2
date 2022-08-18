package environment;

import task.SourceStreamTask;
import task.StreamTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author kevin.zeng
 * @description 全局运行环境
 * @create 2022-08-16
 */
public class RunTimeEnvironment extends Thread{

    //全局环境包含所有的运行实例
    private List<StreamTask<?,?>> tasks;

    public RunTimeEnvironment(){
        tasks = new ArrayList<>();
    }

    public void addTasks(List<StreamTask<?, ?>> tasks) {
        this.tasks.addAll(tasks);
    }

    public void addTask(StreamTask<?,?> task){
        tasks.add(task);
    }

    //TODO 检测运行实例是否正常运行
    public void checkTask(){
        for(StreamTask task : tasks){
            Thread.State state = task.getState();
            System.out.println(task.getName() + "---"+ state);
        }
    }

    public boolean sendCheckPoint(){
        for (StreamTask task: tasks){
            if(task.getName() == "Source"){
                SourceStreamTask sourcetask = (SourceStreamTask) task;
                if(sourcetask.isAlive()){
                    if(sourcetask.sendCheckPointBarrier() == true){
                        System.out.println("success with: runenv send checkpointmsg to sourceStreamTask");
                        return true;
                    }else{
                        System.out.println("Error with: runenv send checkpointmsg to soureceStreamTask");
                        return false;
                    }
                }else{
                    System.out.println("SourceTask has been closed,please recover the source task:" + sourcetask.getName());
                    return false;
                }
            }
        }
        return false;
    }
    @Override
    public void run(){
        while(true){
            try {
                TimeUnit.SECONDS.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //checkTask();
            try {
                TimeUnit.SECONDS.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sendCheckPoint();
        }

    }
}
