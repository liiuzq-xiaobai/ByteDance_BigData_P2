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

    protected List<StreamTask<IN,OUT>> tasks;

    public ExecutionJobVertex(){
        tasks = new ArrayList<>();
    }

    public void addTask(StreamTask<IN,OUT> task){
        tasks.add(task);
    }

    public void setTasks(List<StreamTask<IN,OUT>> tasks){
        this.tasks = tasks;
    }

}
