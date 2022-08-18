package environment;

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

    public void receiveAck(String name) {
        System.out.println("Env receive ACK: " + name + "***checkpoint finished***");
    }
}
