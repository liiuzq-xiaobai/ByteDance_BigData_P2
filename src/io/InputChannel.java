package io;

import record.StreamElement;
import task.ExecutionJobVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author kevin.zeng
 * @description Task的上游输入，一个Task有一个InputChannel
 * @create 2022-08-12
 */
public class InputChannel<T extends StreamElement> {
    //每个InputChannel会接收多个BufferPool提供的数据
    private List<BufferPool<T>> provider;

    //当前管道所属的运行节点
    private ExecutionJobVertex<?,?> vertex;

    //InputChannel使用队列存储数据，供Task消费，如果取不到数据task要阻塞等待
    private BlockingQueue<T> queue;

    //为当前InputChannel绑定数据源buffer
    public InputChannel(){
        queue = new LinkedBlockingQueue<>();
        provider = new ArrayList<>();
    }

    public T take(){
        //从阻塞队列中读数据
        T result = null;
        try {
            result = queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

    public void bindProviderBuffer(List<BufferPool<T>> provider){
        this.provider = provider;
    }

    public int getInputParrellism(){
        return provider.size();
    }

    public void bindExecutionVertex(ExecutionJobVertex<?,?> vertex){
        this.vertex=vertex;
    }

    public void add(T data) {
        try {
            queue.put(data);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
