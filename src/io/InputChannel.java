package io;

import task.ExecutionJobVertex;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author kevin.zeng
 * @description Task的上游输入，一个Task有一个InputChannel
 * @create 2022-08-12
 */
public class InputChannel<T> {
    //每个InputChannel会接收一个BufferPool提供的数据
    private BufferPool<T> provider;

    //记录消费到了BufferPool的哪个位置的数据
//    private int offset;

    //当前管道所属的运行节点
    private ExecutionJobVertex<?,?> vertex;

    //InputChannel使用队列存储数据，供Task消费，如果取不到数据task要阻塞等待
    private BlockingQueue<T> queue;

    //为当前InputChannel绑定数据源buffer
    public InputChannel(){
        queue = new LinkedBlockingQueue<>();
    }

    public T take(){
        //从提供者的buffer中读取数据放到queue中
        //获取当前buffer数据的偏移量
//        int offset = vertex.incrAndGetOffset();
//        T data = provider.take(offset);
//        if(data != null) {
//            try {
//                queue.put(data);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }

        //从阻塞队列中读数据
        T result = null;
        try {
            result = queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

    public void bindProviderBuffer(BufferPool<T> provider){
        this.provider = provider;
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
