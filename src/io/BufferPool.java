package io;

import task.StreamTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author kevin.zeng
 * @description Task生产数据后放入Buffer，每个Buffer对应一个Task
 * @create 2022-08-12
 */
// T为Buffer内的数据类型
public class BufferPool<T> {
    private List<T> list;

    //一个Buffer的数据可以分发给下游多个InputChannel，可以根据数据的哈希值选择发往哪一个InputChannel
    private List<InputChannel<T>> channels;

    public List<T> getList() {
        return list;
    }

    public BufferPool(){
        list = new CopyOnWriteArrayList<>();
        channels = new ArrayList<>();
    }

    public void add(T data){
        list.add(data);
    }

    public T take(int index){
        //防止数组越界
        if(index >= list.size()) return null;
        return list.get(index);
    }

    //为当前数据源绑定一个下游输出
    public void bindInputChannel(List<InputChannel<T>> channels){
        this.channels = channels;
    }
}
