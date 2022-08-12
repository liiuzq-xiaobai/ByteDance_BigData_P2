package task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */

//一个顶点包含多个执行线程
public class ExecutionJobVertex<IN,OUT>{
    //整个顶点一个消费偏移
    private AtomicInteger offset;

    protected List<StreamTask<IN,OUT>> tasks;

    public ExecutionJobVertex(){
        tasks = new ArrayList<>();
        offset = new AtomicInteger(0);
    }

    public void addTask(StreamTask<IN,OUT> task){
        tasks.add(task);
    }

    public void setTasks(List<StreamTask<IN,OUT>> tasks){
        this.tasks = tasks;
    }

    public int incrAndGetOffset(){
        return offset.getAndIncrement();
    }
}
