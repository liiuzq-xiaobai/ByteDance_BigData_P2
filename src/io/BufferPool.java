package io;


import cn.hutool.core.bean.BeanUtil;
import function.KeySelector;
import record.CheckPointBarrier;
import record.StreamElement;
import record.StreamRecord;
import record.Watermark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author kevin.zeng
 * @description Task生产数据后放入Buffer，每个Buffer对应一个Task
 * @create 2022-08-12
 */
// T为Buffer内的数据类型
public class BufferPool<T extends StreamElement> {
    //使用有序集合存储数据
    private List<T> list;

    //一个Buffer的数据可以分发给下游多个InputChannel，可以根据数据的哈希值选择发往哪一个InputChannel
    private List<InputChannel<StreamElement>> channels;

    private int index = 0;
    public List<T> getList() {
        return list;
    }

    public BufferPool() {
        list = new CopyOnWriteArrayList<>();
        channels = new ArrayList<>();
    }

    public void add(T data) {
        list.add(data);
    }

    //在数据发向下游之前，将数据的taskid，也就是来源的task，置为当前task，表示是当前task发送的数据
    public void setCurrentTaskId(StreamElement element){
        element.setTaskId(Thread.currentThread().getName());
    }

    //将数据推向下游
    public void push(T data) {
        push(data, null);
    }

    //对于checkpoint或者watermark类型这种广播的数据，需要在发送到n个下游算子时，
    //创建n个与当前接收到的数据相同的数据，不能共享一个数据
    public StreamElement copyElement(StreamElement element){
        if(element.isCheckpoint()){
            CheckPointBarrier copy = new CheckPointBarrier();
            BeanUtil.copyProperties(element,copy);
            //id属性不拷贝
            copy.setId(element.asCheckpoint().getId());
            return copy;
        }
        if(element.isWatermark()){
            Watermark copy = new Watermark();
            BeanUtil.copyProperties(element,copy);
            return copy;
        }
        return null;
    }

    public void push(T data, KeySelector<StreamElement, String> keySelector) {
        setCurrentTaskId(data);
        //先加入缓冲池
        add(data);
        int channelIndex;
        //checkpoint数据，发送到所有管道当前数据的副本
        if (data.isCheckpoint()) {
            System.out.println(Thread.currentThread().getName() + "****push checkpoint");
            CheckPointBarrier barrier = data.asCheckpoint();
            for (InputChannel<StreamElement> channel : channels) {
                CheckPointBarrier copyBarrier = copyElement(barrier).asCheckpoint();
                System.out.println(Thread.currentThread().getName() + "发送checkpoint副本===" +copyBarrier);
                channel.add(copyBarrier);
            }
        } else if (keySelector == null) {
            //没有分区需求，轮询放置
            channelIndex = index;
            channels.get(channelIndex).add(data);
            index = (index+1) % channels.size();
        } else {
            if (data.isRecord()) {
                //如果是StreamRecord类型且有分区需求，根据哈希值放入对应
                //根据keySelector获取key，根据key的哈希值放入对应管道
                StreamRecord<T> record = data.asRecord();
                String key = keySelector.getKey(record);
                int hash = key.hashCode();
                //保证索引为非负数
                channelIndex = Math.abs(hash % channels.size());
                System.out.println(Thread.currentThread().getName() + "【generate key】 " + key + " " + channelIndex);
                channels.get(channelIndex).add(data);
            } else if (data.isWatermark()) {
                Watermark watermark = data.asWatermark();
                //如果数据是watermark，每个管道都要发
                for (InputChannel<StreamElement> channel : channels) {
                    Watermark copyWatermark = copyElement(watermark).asWatermark();
                    channel.add(copyWatermark);
                }
            } else {
                channelIndex = 0;
                channels.get(channelIndex).add(data);
            }

        }

    }

    //为当前数据源绑定一个下游输出
    public void bindInputChannel(List<InputChannel<StreamElement>> channels) {
        this.channels = channels;
    }
}
